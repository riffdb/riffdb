# Deferred optimizations on dormant paths

Findings from the 2026-09 performance programme that were **deliberately not
acted on**, because the code they concern cannot be reached by any application
in the current build and has no benchmark coverage. Each must be revisited
before the feature that reaches it is activated, so that activation does not
expose a known cost.

The decision to defer rather than fix was made deliberately: with no reachable
caller and no harness coverage, a change here could be neither measured nor
regression-tested. Seven of the eight audit claims examined in this programme
turned out to be inaccurate on inspection, so recording an unverified fix would
have compounded that rather than helped.

## Why these are dormant

- **Tokenized text.** `text_index` is declaration-only in this build, and
  public named tokenized queries are not activated, so no contract surface
  reaches the executor. A server adapter does exist
  (`crates/riffdb-server/src/exact_text_adapter.rs:1202` calls
  `execute_tokenized_text_v1`), so the path is dormant rather than dead: it
  becomes live the moment a contract can name a tokenized query.

  An earlier revision of this page stated that the declaration "maintains
  durable provider state on writes, which is the part that is measurable now".
  That was taken from `docs/contracts/AUTHORING.md` rather than from a
  measurement, and the measurement does not support it. Publishing documents
  against a contract that declares the index, versus one that does not, moves
  no server-side counter: frame bytes per command differ by 0.0, segment bytes
  by 0.0, and writer-busy microseconds by less than the run-to-run spread
  (`benchmarks/perf-surface`). So in this build the declaration has no
  measurable per-write cost -- confirmed at one and thirty-two clients in
  `perf-surface-mechanism-costs-2026-09.md` -- at least with the provider never
  queried; whether population is simply deferred until activation is not
  established either way, and is one more reason to re-measure before the
  feature is switched on.
- **Projection index providers.** Reached whenever a contract declares a
  projection carrying these index types. No contract in the repository
  declares a projection at all, so nothing exercises them.
- **Row-policy evidence.** Reached whenever a contract declares a row policy.
  No benchmark contract declares one, so `allows_policy_record` short-circuits
  on its `None` branch and the scan below never runs.

## Findings, with the code verified as of 2026-09-19

### Tokenized text execution — activate-before-use

- `crates/riffdb-query-executor/src/tokenized_text.rs:109` builds
  `CorpusStatistics` per query; `:188` reads `provider.document_count()` and
  `provider.total_field_length(field)`. **Partially repaired**: `TokenizedTextPartitionIndexV1`
  now maintains `field_totals: BTreeMap<FieldId, u64>` incrementally across
  `apply`, `compact`, and checkpoint restoration so `total_field_length(field)`
  is an $O(\log F)$ lookup instead of an $O(D)$ scan over all documents on every
  query (`total_field_length_tracks_upserts_updates_deletes_compaction_and_checkpoints`).
  Caching `CorpusStatistics` across queries by provider epoch at the trait
  boundary remains open.
- `crates/riffdb-query-executor/src/tokenized_text.rs:126` materialized every
  candidate row before applying offset and limit, so a deep page paid to clone
  the key and the whole `CanonicalRecord` output of every candidate.
  **Repaired 2026-09-19**: ranking now borrows the rows and clones only the
  page that is returned. Every candidate is still scored, because the ranking
  is total over the candidate set and an offset cannot be applied before the
  order exists. `paging_returns_the_same_order_and_rows_as_one_unpaged_page`
  holds a one-row-at-a-time walk against the unpaged page.
- `crates/riffdb-query-executor/src/tokenized_text.rs:253` was recorded here as
  recomputing `CorpusStatistics` inside per-candidate scoring. **That is wrong
  and is withdrawn.** `riff_bm25_v1_score` is a `#[doc(hidden)]` helper whose
  only callers are tests (`tests/tokenized_text.rs:27` and the in-module test
  at `:641`). The production path builds the statistics once in
  `execute_tokenized_text_v1` and threads them through
  `score_with_statistics`, which `review_statistics_are_snapshot_scoped_...`
  already pins at one build per query. Caching across queries by provider
  epoch remains open and is the only live part of this finding.

### Projection index providers — O(N^2) construction (repaired)

- `crates/riffdb-projection/src/long_pattern.rs:138` cloned the entire row map
  and called `rebuild(...)` over all rows on **every single insert**, and `:148`
  did the same on every remove. **Repaired**: incremental postings maintenance
  (`incremental_maintenance_equals_a_full_rebuild_over_the_same_rows`).
- `crates/riffdb-projection/src/exact_predicate.rs` cloned every retained
  row into a fresh `BTreeMap` and rebuilt every order and sparse field index per
  mutation batch. **Repaired**: `ExactPredicatePartitionIndexV4` and
  `ExactPredicatePartitionIndexV5` now apply row mutations in-place against the
  order and sparse field indices with zero-allocation checkpoint size checks and
  atomic rollback on bound failure
  (`incremental_epoch_maintenance_equals_full_rebuild_over_the_same_rows`).
- `crates/riffdb-projection/src/exact_text.rs` cloned the partition index,
  re-sorted touched postings from scratch across every entry, and serialized the
  full checkpoint on every epoch. **Repaired**: `ExactTextPartitionIndexV1`,
  `ExactTextPartitionIndexV2`, and `ExactTextPartitionIndexV3` maintain
  `ascending` and `descending` postings incrementally in sorted order without
  cloning the partition or encoding the full checkpoint on each epoch
  (`filtered_text_incremental_replacement_removal_and_nulls_match_full_rebuild`,
  `randomized_incremental_writes_match_independent_truth_table`).
- `crates/riffdb-projection/src/tokenized_text.rs` cloned the document map,
  rebuilt postings from scratch, and encoded the full checkpoint on every epoch.
  **Repaired**: `TokenizedTextPartitionIndexV1` updates postings in-place and
  validates checkpoint bounds without allocation
  (`incremental_epoch_maintenance_equals_full_rebuild_over_the_same_rows`).

### Row-policy evidence — per-row index scan (repaired)

- `crates/riffdb-query-executor/src/storage_executor.rs` and
  `crates/riffdb-storage-redb/src/query.rs` —
  `indexed_relationship_exists` performs a paged index range scan per
  relationship lookup, and `authorize_candidates`, `allows_policy_record`, and
  `authorize_provider_projected_candidates_admitted_v1` previously called it
  once per lookup per candidate row.

  **Repaired**: Because a row's lookups derive from its field values and the
  snapshot is a pinned owned read view for the whole call, one lookup cannot
  change its answer between rows, and `Eq` on
  `AuthorizedIndexedRelationshipLookupV1` covers the target entity, index,
  partition, and prefix. Each pinned snapshot view (`StorageQueryView`,
  `RedbQueryView`, `authorize_candidates`, `authorize_event_candidates`,
  `authorize_provider_projected_candidates_admitted_v1`,
  `RedbOperationalPorts::authorize_projected_candidates`, and
  `RedbOperationalPorts::inspect_vector_evidence`) now memoizes
  `AuthorizedIndexedRelationshipLookupV1 -> bool` for the lifetime of that
  single snapshot, covered by
  `projected_candidate_admission_deduplicates_identical_relationship_lookups`
  across both `ProviderPolicySourceReaderV1` and `OwnedSnapshotHandle`.

## What must happen before activation

Before public tokenized queries, contract-declared projections carrying these
index types, or contract row policies are activated, each corresponding
finding above must be re-verified against the then-current source, given
benchmark coverage that actually exercises it, and either fixed with a measured
result or explicitly re-deferred with a reason. Line numbers here are pointers
to intent, not contracts; re-locate the code before trusting them.

Note that `crates/riffdb-query-executor/src/storage_executor.rs:511` is *not*
on this list. The per-index-entry `read_entity` there is reachable today
through named `.riffq` queries and is exercised by the `read_only` load; it is
ordinary index-then-fetch, and any change to it belongs in the measured
programme rather than here.
