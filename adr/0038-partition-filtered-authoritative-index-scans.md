# ADR-0038: Partition-Filtered Authoritative Index Scans

- **Status:** Accepted
- **Direction approved:** 2026-07-22
- **Exact text accepted:** 2026-07-22; amended 2026-07-22
- **Accepted:** 2026-07-22
- **Requires:** ADR-0004, ADR-0006, ADR-0007, ADR-0011, ADR-0016,
  ADR-0022, ADR-0029, ADR-0030, ADR-0035, and ADR-0036
- **Amends:** ADR-0004 authoritative index records and scans, ADR-0006 and
  ADR-0022 durable compatibility registry, ADR-0007 obligation application and
  pagination, and ADR-0035 authoritative index-page continuation
- **Amended by:** ADR-0039 for the exact schema source, registries, migration
  type states, bounded evidence, and compare-and-rewrite ownership
- **Decision deadline:** Before WP-130 completion and the P1 gate

The human maintainer accepted this exact decision and its work-package
reconciliation on 2026-07-22.

## Context

The shared service obtains a `PartitionConstraint::Filter` for `ScanIndex`.
ADR-0007 requires that constraint to enter the bounded semantic read request
before lower access and requires returned identities to be checked again. It
explicitly forbids applying the constraint only after an unrestricted scan.

The current storage request carries only an index target, continuation, and row
limit. `StoredIndexEntryV1` carries the index key, historical schema binding, and
covered values, but not the exact logical partition. The WP-130 adapter can
therefore serve only `All`; it correctly rejects an explicit partition scope
before storage rather than performing an unrestricted post-filter. That is safe
but cannot satisfy the accepted explicit-scope semantics.

Storage cannot derive the partition from an index key by itself. The derivation
depends on the exact historical bundle and key schemas selected by the row's
`DurableKeySchemaBindingV1`, and storage is forbidden to interpret contract IR.
A partition hash is also insufficient authorization evidence: policy is defined
over exact canonical `PartitionKey` identity, not probabilistic hash equality.

Filtering can be sparse. A lower scan may inspect a bounded physical page and
find no authorized row. Repeating lower reads inside one RPC until a visible row
appears would make request work depend on the unrestricted range size. A safe
continuation must therefore record physical progress even when the public page
contains no rows.

## Decision

### Exact partition identity in each durable index row

The current durable secondary-index post-image is replaced by the following new
registered payload:

```protobuf
message StoredIndexEntryV2 {
  bytes index_entry_key = 1;
  DurableKeySchemaBindingV1 schema_binding = 2;
  bytes canonical_covered_values = 3;
  bytes partition_key = 4;
}
```

Tags 1 through 3 retain the exact meaning of the V1 fields. Field 4 is required
and contains one complete canonical ADR-0016 `PartitionKey`; a hash, aggregate
ID alone, or independently reconstructed byte string is not a substitute.
`riffdb-storage-api` owns the matching checked `StoredIndexEntryV2` semantic
record. The physical secondary-index table and its complete `IndexEntryKey`
table key do not change.

For every new or changed secondary-index post-image, `riffdb-commit` supplies
the exact partition already retained by
`CommitIntent.pending.partition_key`. It does not rederive the partition from
the index key, current entity state, active catalog, or a hash. Coordinator
validation proves that the historical plan, mutation, index key, and retained
command partition agree before the sequence-free write plan is frozen. The
partition bytes and the complete V2 envelope participate in the existing
pre-sequence capacity reservation and final graph verification.

Deletes continue to identify the complete physical `IndexEntryKey`; they do not
need a second partition parameter. A put and delete for one command remain part
of the same atomic authoritative commit and epoch update. New databases and all
normal writes after this ADR emit V2 only. V1 is never emitted by current code
and is retained solely as a checked migration source.

### Historical validation and restartable offline migration

The first production durable migration is an exclusive startup operation. No
application, administration, worker, or public read/write route becomes ready
while it runs, and no general storage mutation port is exposed.

The startup sequence is:

1. Run the existing exclusive structural evidence pass to exact end, including
   every V1 and V2 secondary-index row and its exact physical key, envelope, and
   `DurableKeySchemaBindingV1`.
2. Have `riffdb-catalog` resolve each binding against the exact retained
   historical bundle. It schema-decodes the index key and embedded entity key,
   derives the historical aggregate partition, and requires full canonical
   consumption. For V2 it requires the derived partition to equal the stored
   exact `PartitionKey`. For V1 it produces a bounded process-local migration
   instruction containing the exact expected V1 row and exact derived V2
   post-image.
3. Through a startup-only migration port, compare and rewrite bounded batches
   at the same physical table keys. The current value must be either the exact
   expected V1 envelope, which is atomically replaced by the exact V2 envelope,
   or the exact expected V2 envelope, which is an idempotent replay success.
   Absence or any other key, binding, covered-value, partition, envelope, or
   schema-hash value is corruption and is not repaired.
4. Resume after interruption by finding remaining V1 rows. Progress is not
   represented by a marker, a new table, or a seventh retained metadata
   category.
5. After no V1 row remains, discard all pre-migration evidence and rerun the
   complete structural and catalog historical validation to exact end. Only
   this post-migration pass may produce the readiness-bearing
   `ValidatedCatalogHistory` and activate dormant operational ports.

Each evidence or migration batch is limited to 500 rows and 4 MiB of complete
encoded content. A batch is atomic; a crash leaves every row in that batch
entirely V1 or entirely V2. Repeating the full startup sequence is required even
after a graceful migration. There is no online, lazy-on-read, or
readiness-concurrent migration.

Catalog owns historical schema selection and partition derivation. Storage owns
structural envelope checks and the exact compare-and-rewrite. The bounded
migration instruction is not `ValidatedCatalogHistory`, a reusable proof, a
generic callback, or an operational mutation capability. No catalog proof or
contract IR crosses a storage trait, and storage does not gain a dependency on
catalog or contract IR.

### Policy-neutral lower partition filter

`riffdb-storage-api` owns a policy-neutral checked lower value with this
semantic shape:

```rust
pub struct IndexPartitionFilter {
    target_lineage: ContractLineage,
    scope: IndexPartitionFilterScope,
}

pub enum IndexPartitionFilterScope {
    All,
    None,
    Explicit(Vec<PartitionKey>),
}
```

`Explicit` contains one through 1,024 unique keys in strict canonical byte
order. Its length-framed key content is at most 1 MiB. `None` represents a
proven-empty intersection without constructing an invalid empty capability
scope or performing a scan. The complete filter constructor also enforces the
existing lineage and request-size bounds.

The shared service remains the sole owner of policy interpretation. Its
consumer-owned authoritative request carries the already resolved effective
scan constraint. The server adapter mechanically converts only
`PartitionConstraint::Filter` into `IndexPartitionFilter`: every explicit
`ScopedPartitionV1` must name the request's exact target lineage, after which
only its canonical `PartitionKey` enters the lower filter. An empty policy
intersection becomes `None`. `PartitionConstraint::Exact`, a mixed lineage,
noncanonical order, duplicate, or over-bound filter fails closed before storage;
it is never treated as `All`.

Storage evaluates the filter while walking the physical prefix. A candidate is
eligible only when its V2 schema binding names `target_lineage` and its stored
exact partition matches `All` or one key in `Explicit`; `None` returns exact end
without inspecting rows. Storage does not import capabilities, principals,
policy decisions, obligations, tenant semantics, or contract IR.

### Bounded sparse-page progress

One lower call returns at most 500 matching entries and at most 4 MiB of encoded
returned content. Independently, it inspects at most 500 physical candidates and
at most 4 MiB of complete encoded candidate content after the exclusive input
continuation. Both ceilings are hard; the requested or policy row limit may only
lower the returned-row ceiling. The registered V2 record bound must ensure that
one maximum-sized legal candidate fits within the inspection-byte budget. A
service RPC performs one such lower scan and does not loop internally to fill a
visible page.

A non-final `AuthoritativeIndexScanPage` carries `scanned_through`, the complete
physical `IndexEntryKey` of the last candidate actually inspected. It may carry
zero matching entries. `scanned_through` must belong to the target prefix, be
strictly greater than the input continuation, and be greater than or equal to
every returned entry key. A non-final result without physical progress is
invalid. Exact end retains its existing explicit final variant and needs no
continuation.

The index epoch and page are still read atomically. The epoch remains the
conservative whole-prefix epoch from ADR-0016 and ADR-0035: a mutation in any
partition under the queried prefix invalidates a cursor. This ADR does not add
partition-specific epoch buckets or relax first-mutation invalidation.

Each lower row returned to the service includes its exact stored partition. The
service schema-decodes the index and entity key under the selected historical
contract and rederives the partition. A mismatch between derived and stored
partition, a row outside the read-time lower filter, or a lineage mismatch is
an internal integrity failure with no partial page or cursor release.

The service then performs the required return-time reauthorization. A row that
was allowed by the read-time filter but is outside a newly narrowed return-time
constraint is omitted; it is not an integrity failure. Field redaction and
response accounting occur only after this check.

When every lower entry considered for the response has been emitted or omitted,
the next lower continuation is `scanned_through`. If response-byte or row limits
stop before an otherwise releasable entry, the continuation is the last emitted
entry instead so no authorized row is skipped. The next call may safely rescan
later candidates. A non-final public `ScanIndex` page may therefore contain zero
rows and a present opaque cursor. Clients must use cursor presence, not row
emptiness, as the end-of-range signal.

The unchanged 16-byte opaque cursor binds at least the principal and capability
context, exact contract/bundle and normalized query identity, index prefix and
fields, observed whole-prefix epoch, prior effective policy, and the selected
physical continuation. It never exposes `scanned_through` or partition keys on
the wire. Fresh policy is intersected with the stored prior policy, so a later
widening cannot reveal rows skipped under an earlier narrower decision.

## Options Considered

1. **`StoredIndexEntryV2`, exact partitions, offline migration, and bounded
   sparse progress:** Accepted. It preserves exact authorization identity,
   storage/IR separation, and bounded work.
2. **Add field 4 under the V1 FQN with only a new schema hash:** Rejected. A new
   V2 FQN makes the incompatible semantic record and migration source explicit
   and prevents same-name old/new payload confusion.
3. **Persist only `PartitionKeyHash`:** Rejected. Hash equality is not exact
   capability-scope evidence and cannot be schema-decoded at return time.
4. **Derive the partition inside storage or on every read:** Rejected. Storage
   lacks the historical IR and must not receive it or a semantic callback.
5. **Scan unrestricted and post-filter in the service:** Rejected by ADR-0007;
   it also makes work and disclosure behavior depend on unauthorized rows.
6. **Maintain a separate key-to-partition table:** Rejected. It adds another
   authoritative reciprocal record, lookup, migration, atomicity edge, and
   corruption mode without improving the row-local proof.
7. **Rewrite the physical index key to include partition first:** Rejected for
   the POC. It changes declared prefix ordering, table keys, cursors, and query
   behavior beyond the necessary durable value migration.
8. **Loop lower scans until the public page is full:** Rejected. Sparse scopes
   could cause unbounded per-RPC work and cancellation latency.
9. **Add partition-specific epochs:** Rejected for the POC. Conservative
   whole-prefix invalidation is correct and avoids a second epoch registry.
10. **Keep rejecting every explicit scan scope:** Rejected as a completion
    strategy. It is fail-closed interim behavior but does not implement the
    already accepted capability and shared-service semantics.
11. **Online or lazy migration:** Rejected. Mixed-format operational reads and
    concurrent commits would expand the recovery and authority boundary.

## Consequences

- The durable compatibility registry accepts 27 FQNs during migration: the
  original 26 plus `riffdb.storage.v1.StoredIndexEntryV2`. The current writable
  semantic set still has 26 roles because V2 replaces V1; V1 is decode-only.
- The complete V2 envelope bound and every aggregate write-plan charge must be
  recomputed. No V1 bound proof may be reused merely because the first three
  fields are equal.
- WP-040 receives a focused review of existing maximum index-delta, affected-
  prefix, and write-plan arithmetic. It adds no language or IR feature; if the
  accepted limits cannot cover the V2 partition charge, implementation stops
  for a separately reviewed bound change.
- Exact-scope scans can return short or empty pages before the range end. This is
  intentional bounded progress, not an availability retry or authorization
  bypass.
- Conservative whole-prefix epochs may invalidate more explicit-scope cursors
  than strictly necessary. The POC accepts that cost.
- No SQL, join, generic transaction callback, storage-to-policy dependency,
  public partition field, or second production mutation path is introduced.

## Compatibility

The public Protobuf schema, RPC inventory, `ScanIndexRequest`,
`ScanIndexResponse`, cursor-token bytes, canonical key formats, hash domains,
contract grammar, and deterministic IR encoding do not change. A response page
with zero rows and a present existing cursor becomes explicitly supported
behavior; clients that inferred end from row emptiness must instead follow the
already authoritative cursor presence.

`StoredIndexEntryV2` has a new registered FQN, descriptor closure, schema hash,
golden payload, semantic decoder, and conservative envelope bound. The outer
`StoredEnvelope` and storage format version remain version 1. The existing
secondary-index table key and ordering remain unchanged. V1 stays registered
only for migration input and is forbidden after successful startup migration.

Migration is restartable, idempotent, and mandatory before readiness. An older
binary presented with V2 refuses its unknown registered tuple rather than
opening or rewriting it; rollback after migration is therefore not supported.
Backup and restore preserve exact V1/V2 bytes and the next open performs the
same migration and full validation rules.

Any later partition-key format, online migration, V1-retirement, physical-key
rewrite, partition-specific epoch, or public cursor change requires another
compatibility decision.

## Security

Exact raw partition keys are authorization-sensitive and remain redacted from
safe errors, logs, metrics, traces, audit summaries, and public cursor material.
Storage sees only the minimum checked lineage and exact key filter needed for
the bounded scan; it never receives a credential, principal, capability, policy
decision, or redaction obligation.

Malformed, mixed-lineage, duplicate, over-bound, unknown-schema, V1-after-
migration, and stored-versus-derived mismatch states fail closed. Return-time
reauthorization and row-level historical derivation prevent a stale read-time
decision or corrupt stored partition from releasing a row. Empty public pages
do not expose how many unauthorized candidates were skipped; only an opaque
continuation and the existing conservative epoch leave the service.

## Testing

- WP-040 reviews compile-time and fixture arithmetic for the extra maximum
  partition charge without widening command-language limits silently.
- WP-050 fixtures derive partitions under exact historical bindings across
  compatible versions, child/root keys, deterministic constants, and retained
  bundles; wrong lineage, version, bundle hash, owner, schema, component,
  trailing bytes, and stored V2 partition fail closed.
- WP-060 memory conformance covers `All`, `None`, and bounded canonical
  `Explicit`; 1,024/1,025 keys; 1 MiB/equal-plus-one; mixed lineages; sparse
  empty non-final pages; strict `scanned_through`; separate 500-row and 4 MiB
  inspected/returned ceilings; cancellation; and atomic epoch/page views.
- WP-065 freezes distinct V1 and V2 descriptors, schema hashes, payload and
  envelope goldens, decode-only V1 registration, V2 field presence, exact
  bounds, malformed cases, and durable decoder fuzz corpus.
- WP-070 tests fresh V2 databases, mixed V1/V2 startup, exact comparison,
  equal-V2 replay, mismatch refusal, no migration marker, bounded batches,
  backup/restore, full post-migration validation, and process failpoints before
  and after every compare-and-rewrite commit.
- WP-100 proves every V2 put uses the exact retained pending partition, every
  delete remains key-only, capacity is charged before sequence assignment, and
  an index/partition mismatch writes no sequence or partial graph.
- WP-120 reference-model and property tests compare every returned row with the
  exact effective scope, cover policy narrowing between read and release,
  integrity mismatches, response truncation, empty public pages with cursors,
  strict progress to exact end, retry, cursor invalidation, and absence of an
  internal refill loop.
- WP-130 public gRPC tests exercise an explicit partition capability, an empty
  page with opaque progress, cursor continuation, and restart over a V1 fixture
  without a transport/storage bypass.
- WP-190 kills and restarts the process throughout migration and sparse scans;
  WP-200 supplies final cross-transport, authorization, migration, recovery,
  generated-artifact, and benchmark evidence.

No correctness test uses sleeps. Deterministic barriers, failpoints, reference
models, generated fixtures, property histories, and process reopen checks cover
the concurrency and durable boundaries.

## Requirements and Work Packages

- **Requirements:** `SYS-004`, `STO-001`, `STO-002`, `STO-012`, `STO-022`,
  `REC-001`, `REC-002`, `API-001`, `SEC-001`, `VAL-003`, `POC-008`, and
  `POC-009`
- **Corrects or blocks:** focused WP-040 bound review; `WP-050`, `WP-060`,
  `WP-065`, `WP-070`, `WP-100`, `WP-120`, and `WP-130`
- **Final evidence:** `WP-190` and `WP-200`

## 2026-07-22 executable migration amendment

ADR-0039 makes this record's migration executable without changing its durable
meaning. V2 resides alone in `proto/riffdb/storage/v1/index_v2.proto`; the nine
existing durable sources and 26 existing tuple fixtures remain byte-identical.
Migration reads exactly 27 registered tuples, while normal writes use exactly
26 roles: 25 unchanged non-index roles plus V2. V1 is decode-only.

WP-065's codec-bound row evidence binds the exact physical key, checked V1 or
V2 semantics, and observed canonical envelope. Concrete storage then binds
pages to the backend, `DatabaseId`, `OpenSessionId`, and continuation. The
initial scan emits `IndexMigrationRow` in the former `0x04` index-entry order,
records only whether V1 exists, and retains no instruction. The bounded rescan
is the sole producer of one fresh row and exactly one closed instruction.

Catalog may depend on the pure bounded invariant evaluator only to evaluate the
exact retained historical aggregate `partition_expression`. Startup joins only
catalog `Ready` with storage `Clean`, or the two matching
`MigrationRequired` values. Migration alternates short storage reads, one-bundle
catalog states, and atomic compare-and-rewrite batches; no storage transaction
spans catalog work. Completion yields a dormant unopened backend and requires a
new full session with a fresh `OpenSessionId`; no marker or readiness proof is
carried across the rewrite pass.

Evidence and instruction/write-batch pages have independent 500-row and 4-MiB
ceilings and must each make progress for one maximum row. General historical
evidence and bundle bounds remain unchanged. WP-060 owns storage type-state and
memory behavior; WP-065 owns schema, registries, codecs, evidence/result
factories, and bound proofs; WP-050 owns catalog derivation; WP-070 owns redb
migration; WP-100 only consumes the exact pre-sequence aggregate-cap result.
ADR-0039 is authoritative for the complete linear protocol and failure classes.

## Decision Deadline

This exact decision is accepted before WP-130 completion. The V2 schema,
migration, policy-neutral lower filter, sparse-page continuation, and required
tests must merge before the P1 gate; the current explicit-scope rejection may
remain only as interim fail-closed behavior.
