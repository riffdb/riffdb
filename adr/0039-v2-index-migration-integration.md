# ADR-0039: V2 Index Migration Integration

- **Status:** Proposed
- **Direction approved:** No
- **Exact text accepted:** No
- **Requires:** ADR-0004, ADR-0006, ADR-0007, ADR-0011, ADR-0016,
  ADR-0022, ADR-0023, ADR-0030, ADR-0035, and ADR-0038
- **Would amend if accepted:** ADR-0004 pre-sequence capacity reservation,
  startup storage type-state, and crate ownership/dependency diagram for the
  narrow catalog-to-invariant edge; ADR-0006 and ADR-0022 durable schema-source
  and registry inventory; ADR-0023 checked derived-index capacity classification;
  ADR-0030 historical-evidence variant, ordering, charge, and coverage;
  ADR-0038 descriptor placement, startup migration integration, historical
  partition derivation, page-bound wording, and pre-sequence capacity-failure
  classification; SPEC Sections 5.2 and 5.3 crate dependency direction,
  Section 9.6 coordinator ordering and capacity result, Sections 10.1 and 10.6
  startup storage/recovery type-state, and Section 10.3 durable registry;
  and the affected WP-050 through WP-130 work-package deliverables
- **Decision deadline:** Before the `StoredIndexEntryV2` durable schema, codec,
  catalog migration derivation, or concrete-engine migration path merges, and
  before WP-130 completion and the P1 gate

This ADR is a review-ready proposal. It is not authoritative unless a human
maintainer accepts its exact text and changes its status to Accepted.

## Context

ADR-0038 accepted the semantic destination: new writes use
`StoredIndexEntryV2`, V1 is a migration-only source, startup performs an
exclusive restartable migration, catalog derives exact historical partitions,
and storage owns exact compare-and-rewrite. Implementation exposed five details
that must be reconciled before those packages can merge independently.

First, adding `StoredIndexEntryV2` to the existing `application.proto` source
would change that file's descriptor. Because a durable record's schema hash is
computed from its source-info-stripped transitive descriptor closure, that
would change schema hashes for existing records whose closures contain
`application.proto`. Existing databases would then present tuples that the new
binary no longer recognizes even though those records did not change.

Second, migration needs each semantic V1 or V2 index row to remain inseparably
paired with its physical table key and exact canonical `StoredEnvelope` bytes.
A public constructor accepting those three independently would let an engine or
test accidentally authorize a rewrite using evidence that the durable codec
never proved belonged together.

Third, the exact historical partition is not encoded in a V1 row. Catalog must
select the row's retained historical bundle, decode the embedded entity key
under that bundle, and evaluate the owning aggregate's accepted
input-computable `partition_expression`. `riffdb-invariant` is the sole shared
implementation of checked expression semantics, while SPEC Section 5.2
currently lists only IR and storage API as `riffdb-catalog` dependencies.
Duplicating expression evaluation in catalog or moving IR interpretation into
storage would violate existing ownership.

Fourth, ADR-0038's phrase "each evidence or migration batch" can be read as
placing a 4 MiB ceiling on general historical evidence containing a contract
bundle. Existing bounds deliberately permit an individual immutable bundle to
exceed 4 MiB and give the general historical-evidence stream its separate
bound. The new 4 MiB ceiling is needed for complete index-row evidence and
migration batches, not as an implicit reduction of the accepted bundle bound.

Fifth, the V2 partition increases real envelope charges. One concrete aggregate
write-set limit can therefore be discovered by the WP-065 codec while the
candidate is still sequence-free. Its caller-visible classification must be
fixed without converting malformed durable data, a noncanonical encoding, a
schema mismatch, or an impossible retained-plan mismatch into transient
availability.

## Decision

### Isolate the V2 descriptor and split readable from writable registries

The new message is defined in exactly:

```text
proto/riffdb/storage/v1/index_v2.proto
```

The source uses package `riffdb.storage.v1`, imports the unchanged
`riffdb/storage/v1/application.proto`, and defines only the accepted
`StoredIndexEntryV2` payload:

```protobuf
message StoredIndexEntryV2 {
  bytes index_entry_key = 1;
  DurableKeySchemaBindingV1 schema_binding = 2;
  bytes canonical_covered_values = 3;
  bytes partition_key = 4;
}
```

`application.proto` and the other eight existing durable source files remain
byte-identical. The canonical descriptor closure and schema hash of every
existing registered payload, including `StoredIndexEntryV1`, must therefore be
byte-identical to the accepted 26-record fixtures. Only
`riffdb.storage.v1.StoredIndexEntryV2` receives a new closure and schema hash.

During migration the readable durable registry contains exactly 27 tuples: the
accepted 26 plus V2. The writable role registry contains exactly 26 entries:
the unchanged 25 non-index roles plus V2 in the current index-entry role. V1 is
readable only and has no current encoder or normal-write lookup. A registry
count is not allowed to stand in for these role assertions; tests must prove
the exact ordered FQNs, hashes, and readable/writable membership.

The outer `StoredEnvelope`, storage format version, physical secondary-index
table, and complete physical `IndexEntryKey` are unchanged.

### Bind migration row evidence at the durable codec boundary

WP-065 owns a process-local evidence value for one migration-scan row. It binds
all of the following as one checked unit:

- the exact physical secondary-index table key;
- the semantically decoded `StoredIndexEntryV1` or `StoredIndexEntryV2`; and
- the exact canonical registered `StoredEnvelope` bytes from which that row was
  decoded.

Its production constructor or factory is available only to the storage-owned
durable codec path. Construction proves the registered tuple, checksum, schema
hash, canonical wire form, semantic bounds, physical-key/record-key equality,
and V1-or-V2 variant before returning evidence. General storage consumers,
catalog, concrete engines, and tests cannot assemble production evidence from
independent values or substitute a re-encoding for the observed envelope.

This codec evidence proves only the canonical association among the physical
key bytes presented to the codec, the exact envelope bytes presented with that
key, and the decoded semantic row. It does not by itself claim that either byte
string came from a filesystem, table, transaction, or particular backend. The
sealed concrete-backend startup session owns that provenance: it reads the row
under its short transaction, invokes the private codec factory, and wraps the
result in a page bound to that backend, `DatabaseId`, `OpenSessionId`, and
continuation. No caller can attach that session origin after the fact.

The initial historical-evidence scan and the startup migration port both
produce this same codec evidence inside backend/session-bound pages. The
concrete backend may move it through its exclusive startup implementation, but
it does not interpret the row's contract IR. Catalog may inspect the checked
semantic fields needed for historical derivation and consume the canonical
association into one bounded migration instruction. The instruction is one of
exactly two closed variants:

- `V1Rewrite` binds the complete codec-checked expected V1 evidence (physical
  key, semantic row, and exact canonical envelope) and the exact derived
  semantic V2 post-image. The storage-owned codec remains the only producer of
  the canonical V2 replacement envelope.
- `V2Confirm` binds the complete codec-checked expected V2 evidence after
  catalog has proved its stored partition equals the historical derived
  partition. It authorizes no write.

There is no untyped instruction, catch-all variant, independently constructible
expected value, or instruction that omits the observed envelope association.
Both variants are process-local, session-bound, move-only values, not durable
proofs or general mutation requests.

The initial scan carries one checked row only as the new closed
`HistoricalSemanticEvidence::IndexMigrationRow` variant. That variant replaces,
and never accompanies, the former
`PersistedKey(IrOpaquePersistedKeyV1::IndexEntry)` item for the same physical
row. After this amendment a `PersistedKey` stream item is valid only for an
entity key or index-range prefix; the public index-entry evidence constructors
are removed or made codec-private. A backend emits exactly one migration-row
item for every physical V1 or V2 index row, and catalog counts and consumes it
exactly once: first as the existing historical `KeySchema` check, then as the
format/partition check and possible migration instruction. It never consumes a
second synthesized persisted-key item.

The new variant remains in the existing `0x04` ordering domain. Its exact order
key is byte-for-byte the former persisted-index-key order key:

```text
0x04
|| u32_be(lineage_length) || lineage
|| u64_be(contract_version) || 32-byte bundle_hash
|| 0x02 || u32_be(index_id)
|| u32_be(index_entry_key_length) || index_entry_key
```

Physical-key/record-key equality makes that final key the checked physical key.
The capability-partition tag remains `0x05`; every entity, range-prefix, and
index-migration `0x04` item still precedes every capability-partition item. The
new item's checked general-page semantic charge is exactly
`1 + 4 + physical_key_length + 4 + canonical_envelope_length`, with every
addition checked. Its simultaneously enforced migration-row sub-bound counts
that same charge toward the independent 500-row/4-MiB ceiling. The semantic row
is a checked interpretation of those charged envelope bytes, not a second
chargeable stream item. Ordering, pagination, exact-end coverage, and charge
fixtures must reject an old-plus-new duplicate, a missing row, either version's
wrong discriminator, and any `0x04` item after `0x05`.

### Permit one narrow catalog-to-invariant dependency

`riffdb-catalog` may depend directly on `riffdb-invariant` only for pure,
bounded evaluation of the owning aggregate's historical
`partition_expression` while validating or migrating a checked historical
index row. Catalog remains responsible for exact historical bundle and schema
selection. `riffdb-invariant` remains responsible for expression semantics.

The evaluator receives only the selected immutable checked IR and already
schema-decoded canonical root-key values. This use performs no storage access,
network or filesystem I/O, clock read, entropy, process-global mutation,
callback, transaction access, command execution, or asynchronous operation. It
does not give catalog a runtime, commit-check, or storage-mutation role. There
is no reverse `riffdb-invariant` dependency on catalog and no new storage-to-IR
dependency.

For a retained entity key, catalog resolves the exact entity and its one
historical aggregate owner. It validates and decodes the complete entity key
under the retained `KeySchema`. The aggregate root's complete primary-key
schema supplies the required prefix length and types:

- for a root entity, that prefix is the complete root key;
- for a child entity, it is exactly the first root-key-component-count decoded
  child-key values; and
- for an index row, the same rule is applied to the complete embedded entity
  key after the outer index components and embedded key have both been consumed
  exactly.

Root component position `i` is bound to decoded prefix value `i`. Derivation
does not search child fields by name, read the entity payload or covered values,
use the active bundle, infer a join, or accept an incomplete/trailing key. The
accepted ADR-0016 compiler rule that child keys begin with the exact root schema
already proves matching names, types, and order; startup revalidates that rule
against the selected historical bundle. Catalog evaluates the aggregate key
plan's exact historical `partition_expression`, encodes its result under the
exact historical partition schema, and constructs the complete exact
`PartitionKey` for that aggregate.

For V1, the derived key becomes field 4 of the exact expected V2 post-image. For
V2, it must equal the stored field 4. A wrong owner, root prefix, schema,
lineage, version, bundle hash, expression result, stored partition, incomplete
consumption, or trailing byte is fatal historical corruption; it is never
repaired by guessing from current catalog state.

### Use a linear, session-bound migration port

The migration path remains an exclusive startup-only capability and is linear
by construction. It is not added to any operational storage trait.

1. One normal startup session completes the full structural and historical
   evidence streams to their backend-owned exact-end tokens. Storage and
   catalog independently record whether any checked V1 index row was observed.
   No operational or mutation port is released.
2. Consuming catalog validation returns exactly
   `CatalogHistoryOutcome::Ready(ValidatedCatalogHistory)` when the complete
   stream is V2-only, or
   `CatalogHistoryOutcome::MigrationRequired(CatalogIndexMigrationContext)`
   when at least one V1 was observed. The latter context is deliberately a
   non-readiness type: it has no conversion to `ValidatedCatalogHistory`, no
   current-recheck operation, and no operational accessor.
3. Consuming the storage session and both exact-end tokens returns exactly
   `StructuralOpenOutcome::Clean(StructurallyOpened)` when the backend observed
   no V1, or
   `StructuralOpenOutcome::MigrationRequired(StartupIndexMigrationPort)` when
   it observed at least one V1. The port remains bound to the same `DatabaseId`
   and `OpenSessionId`. The server may join only `Ready` with `Clean`, or consume
   both `MigrationRequired` values into the migration driver. Either crossed
   pair is an integrity failure. There is no public constructor, reusable
   authority, generic callback, fallback conversion, or parallel migration
   handle.
4. The port scans V1 and V2 physical index rows in strict physical-key order
   through short-lived read transactions. Each page returns backend/session-
   bound codec evidence and an explicit, strictly advancing continuation or
   exact end. The paired migration driver also provides a same-session bounded
   historical-bundle point-read path for the exact retained bundle reference of
   the current checked row; neither the catalog context nor the storage port
   alone gains the other layer's authority. The point-read consumes the linear
   driver into a one-bundle state. The backend opens a short read transaction,
   reads that one immutable bundle, closes the transaction, and only then
   returns its owned, canonically checked bytes to catalog. Catalog must consume
   that state into the row's instruction before the driver is returned; dropping
   it aborts migration. The request cannot name an unvalidated reference or a
   row outside the current page, and one response is bounded by the accepted
   15 MiB bundle limit. The context may retain only its accepted bounded
   reference/digest proof, never an unbounded collection of bundle bytes. No
   storage transaction is held while catalog decodes a key, evaluates an
   expression, or retains an instruction batch.
5. Catalog consumes each row with its exact same-session bundle and produces
   exactly one `V1Rewrite` or `V2Confirm` instruction. It cannot skip a row,
   issue two instructions for one row, revisit a consumed row, or complete the
   page until every row has one instruction. The port consumes the page and its
   closed instruction batch together; stale, reordered, repeated, cross-page,
   cross-database, or cross-session values fail closed.
6. Storage opens one short write transaction for the whole instruction batch.
   It constructs every canonical V2 replacement through the durable codec,
   compares every current table value, and commits all authorized replacements
   once or none. For `V1Rewrite`, exact expected V1 is replaced by the exact
   derived V2 envelope, while that exact V2 envelope is an idempotent replay
   success. For `V2Confirm`, only the exact expected V2 envelope succeeds and
   no write occurs. Absence or any other key, envelope, binding, covered value,
   partition, or schema hash aborts the whole batch as corruption. A crash
   cannot expose a proper subset of one batch's committed replacements.
7. The port can reach its backend-private exact end only after each page and
   batch has been consumed once and the complete physical range has been
   visited. Only consuming the catalog context after its last row can produce
   `CatalogIndexMigrationCompletion`; that type is not catalog readiness.
   Finishing consumes the port, backend exact-end token, and that exact
   same-session completion and returns only a dormant unopened backend state.
   It cannot return `StructurallyOpened`, `ValidatedCatalogHistory`, operational
   ports, or readiness.
8. The server discards all pre-migration evidence, contexts, and proofs, begins
   a new startup session with a fresh `OpenSessionId`, and repeats the complete
   structural and catalog historical validation to exact end. Only a V2-only
   `Ready` plus `Clean` pair from that fresh session may participate in the
   existing readiness join. A second `MigrationRequired` result immediately
   after an in-process completed migration is an integrity failure; after a
   crash, restart normally begins again at step 1.

A crash or cancellation drops the linear port and releases no operational
authority. Restart begins again at step 1. There is no migration marker, new
metadata category, retained process-local continuation, online migration, or
readiness concurrent with migration.

Complete encoded V1/V2 index-row evidence pages and migration instruction/write
batches each retain independent ceilings of 500 rows and 4 MiB. The evidence
page's checked charge is the length-framed physical key plus exact observed
canonical envelope for every row, including fixed per-item framing. The
instruction/write batch's checked charge is the length-framed physical key,
exact expected canonical envelope, closed instruction discriminator/framing,
and the codec's conservative complete canonical V2 replacement-envelope charge
for every row. `V2Confirm` reserves that replacement slot even though it cannot
write. Every addition is checked; neither decoded allocations nor a smaller
actual replacement may erase one of these charged components.

WP-065 must prove from the accepted key, value, binding, payload, envelope, and
framing maxima that one maximum valid row plus its conservative V2 replacement
fits in 4 MiB. A semantic row is never split. Failure of that proof is an
authoritative-bound conflict requiring human review, not permission to raise a
limit or loop without progress. The 500-row and 4-MiB ceilings cannot be raised
by a caller. They do not reduce the existing immutable contract-bundle bound or
the separately bounded general historical-evidence stream. In particular, a
valid historical bundle is not rejected merely because it exceeds 4 MiB while
remaining within its already accepted bundle and historical-page bounds.

### Classify one sequence-free concrete aggregate limit narrowly

The durable codec must compute every conservative complete V2 envelope
upper-bound charge and their aggregate for the exact frozen
`CommandWriteSetPlanV1` before application-sequence assignment. The accepted
reservation and retained-plan equality checks still run; after sequence
assignment the codec still checks every final actual canonical envelope against
that reservation before staging. This decision does not move capacity work
after sequence assignment or relax any bound.

The codec boundary does not expose one undifferentiated `LimitExceeded` for this
decision. It first computes and validates each complete per-record charge, then
checked-adds every charge, and only after those operations succeed compares the
final aggregate with the accepted 16 MiB staged-write cap.
`riffdb-storage-api` owns a fields-private closed result whose public variants
distinguish `Fits(EncodedWriteSetUpperBound)` from
`ExceedsAcceptedAggregateCap`; `riffdb-commit` is its only production consumer.
Every failure while encoding or charging a record, every per-record limit
failure, and every checked-sum overflow remains the existing typed codec error
and cannot produce the aggregate-cap variant. The aggregate-cap variant is
constructible only inside the codec at that one final comparison.

The commit candidate privately converts only
`ExceedsAcceptedAggregateCap` into its origin-specific
`CapacityUnavailable` decision. Only that decision makes the command executor
return the existing public `StorageUnavailable`. The backend has proved that no
authoritative write was attempted or committed: the durable Pending row remains
byte-identical, no sequence is assigned, no terminal
outcome/event/provenance/outbox/commit graph is written, the coordinator is not
fenced or stopped, and authoritative readiness remains true. The response
exposes no calculated size or record content and retains the existing safe
retry guidance.

This is not a general mapping from semantic limits or codec failures to
availability. Generic or per-record `LimitExceeded`, checked-charge overflow,
malformed or noncanonical encoding, unknown type/hash/version,
key/envelope mismatch, reservation undercharge, retained-plan substitution, and
every other codec or integrity error are fatal internal integrity failures.
They write no sequence or partial graph, stop authoritative command processing,
and keep readiness false under the existing coordinator lifecycle. Exhaustive
matching on the private origin-specific result is required; code may not inspect
an error string or map `DurableCodecErrorKind::LimitExceeded` by itself. A write
error after sequence assignment or commit attempt continues to use the accepted
proven-rollback or uncertain-outcome rules; this ADR does not reclassify it.

## Options Considered

1. **Isolated V2 source, split registries, codec-bound evidence, catalog's
   narrow shared evaluator use, and linear offline migration:** Proposed. This
   preserves old schema hashes and existing ownership while making the first
   production migration executable and restartable.
2. **Append V2 to `application.proto`:** Rejected. It changes descriptor
   closures and hashes for unchanged existing durable records.
3. **Treat both V1 and V2 as current writable records:** Rejected. It permits
   normal writes to recreate the migration source and prevents a clean
   post-migration invariant.
4. **Let an engine or catalog assemble row evidence from separate semantic and
   envelope values:** Rejected. Exact compare-and-rewrite would rely on a
   forgeable association outside the canonical decoder.
5. **Duplicate expression evaluation in catalog or evaluate IR in storage:**
   Rejected. Either creates semantic drift or violates the IR-blind storage
   boundary.
6. **Derive child partitions through field-name lookup, covered values, or the
   active bundle:** Rejected. Historical ownership is the exact positional root
   key prefix under the row's retained bundle.
7. **Hold one storage transaction across catalog derivation or retain all rows
   in memory:** Rejected. It expands lock duration and loses the bounded
   page-by-page separation between evidence, pure derivation, and mutation.
8. **Allow migrated state to become ready without a fresh full pass:** Rejected.
   Pre-migration evidence cannot prove the complete post-migration durable
   state.
9. **Apply 4 MiB to every historical page or bundle:** Rejected. It silently
   weakens the accepted contract-bundle boundary and strands valid histories.
10. **Map every codec failure to `StorageUnavailable` or every aggregate limit
    to a fatal stop:** Rejected. The former hides corruption as retryable; the
    latter unnecessarily disables a database after a proven sequence-free
    capacity refusal.

## Consequences

- Generated durable-source inventory grows from nine to ten files, while all
  existing per-record descriptor closures, schema hashes, payload goldens, and
  envelope goldens remain stable.
- Durable APIs must name readable and writable role registries explicitly.
  Existing ambiguous `current` naming may be retained only where its semantics
  are exactly the 26 writable roles; migration decoding uses the 27-entry
  readable registry.
- `riffdb-catalog` gains one direct dependency on `riffdb-invariant`. If this
  ADR is accepted, SPEC Sections 5.2 and 5.3 and WP-050 architecture checks must
  record the exact exception before the dependency merges.
- Startup migration performs at least two complete validation passes when V1
  exists and may scan the index range again for bounded rewrite. This startup
  cost is accepted for the POC in exchange for no online mixed-format path.
- Migration remains restartable without new durable metadata. Repeated V2
  observations are exact idempotent replay, not silent overwrite.
- General historical bundle/page limits remain unchanged. Index migration adds
  its own independently tested 500-row/4-MiB boundaries.
- One narrowly proven pre-sequence aggregate capacity refusal remains
  retryable without making malformed or contradictory codec state retryable.
- No public gRPC or MCP message, cursor, URI, contract grammar, executable IR,
  plan hash, storage table key, or outer envelope changes.

## Compatibility

The existing 26 durable record tuples are compatibility fixtures and remain
readable under their exact accepted schema hashes. V2 is an additive registered
FQN in a new source file. During migration, V1 and V2 may coexist physically;
after the clean post-migration validation, V1 is forbidden and all normal index
puts encode V2.

An older binary presented with V2 continues to refuse the unknown tuple.
Rollback to that binary after migration is unsupported. Backup and restore
preserve exact envelope bytes; opening a restored mixed or V1 database invokes
the same exclusive migration and fresh-validation sequence. No new backup
format or online backup behavior is decided here.

The proposed catalog dependency is process-local and changes no durable or
public format. Positional root-prefix derivation implements the already accepted
ADR-0016 key convention; it does not add a child-to-root mapping or language
construct.

The existing `StorageUnavailable` public kind, code, safe message, recovery
action, and transport mapping are reused unchanged. No new public error kind or
durable failure record is added.

## Security

Exact partition keys, index keys, covered values, envelope bytes, and migration
instructions are authorization-sensitive process-local data. They remain
redacted from public errors, logs, metrics, traces, health text, and incident
narratives. Safe diagnostics expose only closed classifications and opaque
incident IDs where already supported.

Codec-bound evidence prevents downstream storage consumers, catalog, and tests
from recombining a valid semantic row with different canonical bytes. The
sealed concrete-backend session, not the codec value alone, binds that checked
association to the row actually visited in the session's ordered scan. Exact
compare-and-rewrite prevents migration from becoming a general startup mutation
path. Catalog sees only already checked row material needed for historical
derivation; storage receives exact expected bytes and never receives IR, a
policy decision, credential, principal, capability, or semantic callback.

Unknown historical owners, bindings, schemas, descriptor tuples, V2 partition
mismatches, noncanonical keys, stale migration instructions, and post-migration
V1 rows fail closed before readiness. The narrow capacity mapping discloses no
size oracle and cannot convert corrupt or attacker-crafted durable input into a
retryable public response.

### Corrective ownership and sequencing

This decision is a coordinated correction across packages that have already
started; it does not reverse their declared hard-dependency DAG. Ownership and
merge sequencing are exact:

1. WP-060 first owns the fields-private process-local migration-row evidence
   wrapper, the new `HistoricalSemanticEvidence` variant, its `0x04` ordering and
   charge rules, sealed session/page carriage, and memory type-state surface in
   `crates/riffdb-storage-api/src/startup.rs` and the existing memory paths. That
   interface alone grants no constructor capable of asserting codec evidence.
2. WP-065, which already depends on WP-060, owns `index_v2.proto`, generated
   descriptors, readable/writable registries, and the storage-API
   `proto_codec` factory that is the sole production constructor of the wrapper.
   Its generation, codec, architecture, and durable-decoder fuzz checks merge
   before any consumer claims migration integration complete.
3. Only after the WP-065 factory is fixed may the coordinated consumer PRs land:
   WP-050 owns historical catalog validation and partition derivation; WP-060
   owns memory production/conformance; and WP-070 owns redb production, the
   linear migration port, compare-and-rewrite, and process recovery. This is an
   additional soft sequencing constraint for the correction, especially for
   WP-050, and creates no WP-060/WP-065 dependency cycle.
4. WP-100 owns only the sequence-free aggregate-cap result and commit mapping.
   WP-120 and WP-130 consume the completed startup/read-filter behavior and add
   no migration constructor or semantic owner.

The authoritative reconciliation adds ADR-0039 to every affected package's
`required_adrs`, records the exact package-owned edits within the existing
allowed paths described above, and preserves every existing acceptance command.
It additionally retains WP-065's
`generate-proto --check` and `proto_durable` fuzz command; no package may treat a
different owner's passing tests as a substitute for its own acceptance evidence.

## Testing

- WP-065 generation checks freeze `application.proto` and all prior descriptor
  closures/hashes byte-for-byte, add exactly `index_v2.proto`, and prove the V2
  FQN, fields, closure, schema hash, payload/envelope goldens, and conservative
  bound.
- WP-065 registry tests prove exact 27-readable/26-writable membership and
  ordering, V1 decode-only behavior, V2-only normal encoding, unknown tuples,
  and no ambiguous FQN/hash pair. Decoder fuzzing includes both versions.
- WP-065 codec tests prove the migration evidence factory accepts only a
  canonical envelope whose decoded record key equals the presented physical
  key. Wrong key, changed payload, CRC, hash, version, noncanonical encoding,
  variant, and any replacement with different bytes fail. Separate WP-060 and
  WP-070 tests prove only a sealed backend session can attach the exact bytes
  observed by its short read transaction and that a page cannot be rebound to
  another database, session, or continuation. A byte-identical canonical
  re-encoding is intentionally indistinguishable at the codec boundary.
- WP-060/WP-065/WP-050 fixtures freeze the replacement-only
  `IndexMigrationRow` variant, byte-exact retained `0x04` order key, exact charge,
  V1/V2 coverage, single catalog consumption, and unchanged `0x05` capability
  ordering. They reject duplicate old-plus-new index evidence, omission,
  reordering, pagination gaps, and a migration row after capability evidence.
- WP-050 tests derive the same partition for root and child keys using exact
  positional root prefixes, deterministic constants, and retained historical
  bundles. Index fixtures cover exact outer-index and embedded-entity-key
  consumption. Wrong owner, prefix length/order/type, lineage, version, bundle
  hash, aggregate, schema, stored V2 partition, and trailing bytes fail closed.
- Architecture tests permit only the declared `riffdb-catalog` to
  `riffdb-invariant` edge and prove no inverse edge, storage-to-IR edge, callback,
  storage handle, clock, entropy, async type, or runtime command API crosses the
  pure evaluator boundary.
- WP-060 memory type-state tests prove migration entry requires the same
  session's two exact-end authorities; handles are linear; stale, skipped,
  repeated, reordered, cross-database, and cross-session pages/instructions
  reject; `Ready` joins only `Clean`; the two `MigrationRequired` values are
  non-readiness types and must be consumed together; crossed outcomes reject;
  and no operational port is available during or after migration.
- WP-060 and WP-070 tests prove the migration bundle point-read accepts only the
  current row's exact same-session retained reference, holds no transaction
  during catalog evaluation, returns at most one bounded owned bundle at a
  time, and rejects stale, arbitrary, cross-page, and cross-session requests.
- WP-060 and WP-070 boundary tests cover 500/501 rows and 4 MiB/equal-plus-one
  for complete index-row evidence and migration batches. They assert the exact
  physical-key, expected-envelope, discriminator/framing, and conservative V2
  replacement charges, and prove one maximum valid row fits without chunking,
  while a valid greater-than-4-MiB historical bundle remains accepted under its
  unchanged separate bounds.
- WP-070 memory/redb conformance covers fresh V2 databases, all-V1 and mixed
  V1/V2 databases, strict physical order, short read transactions, exact V1-to-
  V2 replacement, exact V2 replay, `V2Confirm` no-write behavior, mismatch
  refusal, whole-batch all-or-none commit, no marker, cancellation, and a final
  database containing no V1.
- WP-070 process failpoints before and after each bounded compare-and-rewrite
  commit prove restart from step 1. Tests prove migration completion returns
  only dormant unopened state, the next pass has a fresh `OpenSessionId`, and
  readiness requires complete post-migration structural and catalog exact ends.
- WP-100 tests recompute conservative complete V2 envelope upper-bound charges
  and their aggregate before sequence assignment, then check final actual
  canonical envelopes against the reservation before staging. Only the
  origin-specific final
  `ExceedsAcceptedAggregateCap` comparison returns existing
  `StorageUnavailable` with byte-identical Pending, no sequence/graph, no
  fence/stop, and readiness preserved. Exhaustive tests prove per-record limits,
  checked-sum overflow, and every generic codec error stop command readiness as
  an internal defect with no partial write.
- WP-120 and WP-130 tests prove migrated V2 rows participate in the accepted
  lower partition filter and return-time reauthorization without a
  transport/storage bypass, including restart from a V1 fixture.
- WP-190 supplies process-kill coverage throughout migration. WP-200 supplies
  final generated-artifact, recovery, authorization, cross-transport, and POC
  exit evidence.

No correctness test uses sleeps. Session identities, explicit barriers,
failpoints, deterministic schedules, golden fixtures, fuzzing, and process
reopen checks provide the evidence.

## Requirements and Work Packages

- **Requirements:** `SYS-004`, `STO-001`, `STO-002`, `STO-012`, `STO-020`,
  `STO-021`, `STO-022`, `REC-001`, `REC-002`, `TXN-041`, `TXN-043`,
  `API-001`, `SEC-001`, `VAL-003`, `POC-008`, and `POC-009`
- **Interfaces corrected or blocked:** `WP-050`, `WP-060`, `WP-065`, `WP-070`,
  `WP-100`, `WP-120`, and `WP-130`
- **Final recovery and exit evidence:** `WP-190` and `WP-200`

If accepted, the affected work-package ADR dependencies, allowed dependency
statements, deliverable text, and acceptance fixtures must be reconciled before
implementation is claimed complete. This ADR does not remove any declared hard
dependency or acceptance command.

## Decision Deadline

The exact text must be accepted or rejected before the V2 durable source and
codec, catalog-to-invariant dependency, concrete migration port, or narrow
capacity-error mapping merges. All accepted pieces and their fixtures must be
implemented before WP-130 completion and the P1 gate. Until then, V1 migration
and explicit-scope completion remain incomplete; no Proposed statement in this
document authorizes an implementation or specification change.
