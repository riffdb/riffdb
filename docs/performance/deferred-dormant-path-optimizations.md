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
  measurable per-write cost, at least at one client with the provider never
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
  folds per-field lengths. The statistics are a function of the provider epoch,
  not of the query, so they can be computed once per epoch and reused.
- `crates/riffdb-query-executor/src/tokenized_text.rs:126` materializes every
  candidate row before applying offset and limit, so a deep page pays for the
  whole candidate set.
- `crates/riffdb-query-executor/src/tokenized_text.rs:253` recomputes
  `CorpusStatistics` again inside per-candidate scoring.

### Projection index providers — O(N^2) construction

- `crates/riffdb-projection/src/long_pattern.rs:138` clones the entire row map
  and calls `rebuild(...)` over all rows on **every single insert**, and `:148`
  does the same on every remove. Building N rows is therefore O(N^2). This is
  the clearest of the findings and was verified directly in the source.
- `crates/riffdb-projection/src/exact_predicate.rs:534` clones every retained
  row into a fresh `BTreeMap` per mutation batch. Batched rather than
  per-insert, so less severe than the above, but the same shape.
- `crates/riffdb-projection/src/exact_text.rs` carries the equivalent rebuild.

### Row-policy evidence — per-row index scan

- `crates/riffdb-query-executor/src/storage_executor.rs:928` —
  `indexed_relationship_exists` performs a full paged index range scan per
  relationship lookup, and `allows_policy_record` calls it once per lookup per
  candidate row. With a row policy active this is a scan per row, bounded only
  by `MAX_QUERY_SCANNED_ROWS`.

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
