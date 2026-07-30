# WP-375 partition/index generations

Status: implementation and safety evidence; PERF-008 PostgreSQL parity remains
a measured release gate.

## Product rule

RiffDB invalidates indexed reads conservatively at exactly one durable identity:

```text
(canonical aggregate partition key, stable index ID)
```

Every index range read is bound to that identity and its observed generation.
Every command that changes any entry in the same partition and index advances
the generation once, regardless of how many entries or index-key components the
command changes. Old and new keys are deduplicated into the same target.

Application code cannot select a narrower invalidation bucket. RiffQL must prove
one exact partition route before planning an indexed read. A query without that
proof fails with the existing source-spanned `RDB-QP002` locality diagnostic.
The command language exposes no arbitrary range read; any future
write-influencing range construct must prove the same bounded generation before
it can enter command IR.

This rule deliberately accepts false-positive retries when unrelated ranges in
the same partition/index change. It never accepts a stale range merely because
the mutation fell outside a prefix bucket.

## Cursor and dependency safety

Kernel range targets now contain both the complete index prefix and the exact
partition/index generation target. Filtered scans accept exactly one explicit
partition, and reject `all`, `none`, or multi-partition scopes at construction.
Snapshot validation compares the same pair and generation.

RiffQL access plans carry the compiler-derived partition key schema. Query
execution derives the canonical partition from the query's exact route
parameter and reads the generation in the same snapshot as result rows. Stable
cursors already bind the query plan hash; the query-program encoding includes a
reviewed generation-model marker, so pre-WP-375 cursors fail closed.

## Durable format and migration

`StoredIndexGenerationV2` replaces writable `StoredIndexEpochV1` rows. Its
durable key is:

```text
u32-be partition-key length || canonical partition-key || u32-be index ID
```

The compact durable identity is tag 10, revision 2. Revision 1 prefix-epoch
rows remain readable only for migration.

Migration is bounded, restartable, and ordered:

1. retain the pre-generation registry digest;
2. compute the maximum historical generation for each index;
3. scan catalog-validated V2 index rows and create one pair row at
   `historical maximum + 1`;
4. durably remove legacy prefix rows in bounded batches;
5. validate that every remaining generation key and payload name the same pair;
6. publish the current registry digest last.

Existing pair rows contribute to the conservative maximum, which makes a
partially completed migration idempotent. An index row with neither historical
nor current generation evidence fails closed as corrupt data. V1 index rows are
first rewritten by the catalog-owned migration so their authoritative
partition is known; storage does not guess it.

Migration and recovery commits use the hardened immediate two-phase profile.
Normal application commits retain the standard immediate one-phase profile.

## Evidence

Automated evidence covers:

- command derivation deduplicating old/new index keys to one pair;
- distinct indexes producing one target each;
- memory and redb range/snapshot generation validation;
- exact-partition scan construction and rejection of unprovable scopes;
- durable V2 schema, registry, compact identity, and generated fixtures;
- legacy catalog/index migration, bounded batches, restart behavior, and
  registry-last publication;
- semantic command, recovery, query compiler, server adapter, and generated
  artifact suites.

`./scripts/benchmark-command-growth --assert-perf-012` runs those semantic and
crash preflights before emitting a machine-readable generation summary. The
complete PERF-008 concurrent and same-run PostgreSQL comparisons remain
separate gates; a semantic pass does not claim performance parity.

## Known cost tradeoff

The model removes prefix-length write fan-out and hot global prefix rows. A
command now writes at most one generation row per distinct affected
partition/index pair. Reads may retry more often than a prefix-specific scheme,
but the invalidation cost is bounded, independent of index arity, and safe by
construction.
