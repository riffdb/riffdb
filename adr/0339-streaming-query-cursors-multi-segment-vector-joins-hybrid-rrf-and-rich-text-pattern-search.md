---
adr: "0339"
title: Streaming Query Cursors, Multi-Segment Vector Joins and Hybrid RRF, and Stemmed/Fuzzy/Regex/Snippet Search
status: accepted
tier: surface
date: 2026-11-06
accepted: 2026-11-06
acceptance: 'maintainer, in session, 2026-11-06: approved implementation_plan.md ("approved, lets build")'
requires: [ADR-0086, ADR-0091, ADR-0158, ADR-0173, ADR-0174, ADR-0229, ADR-0255]
amends: [ADR-0091, ADR-0173, ADR-0174, ADR-0255]
supersedes: []
requirements: [QRY-001, QRY-002, QRY-004, PRJ-001, PRJ-004, VEC-001, VEC-004, VEC-006, VEC-007, VEC-008, VEC-009, VEC-010, VEC-011]
packages: [WP-909, WP-910, WP-911, WP-912]
obligations:
  - id: OBL-0339-1
    package: WP-909
    proof: streaming_cursor_paginates_across_all_named_query_providers_with_pinned_snapshot
    description: Named queries across projected-table, exact-predicate, exact-index, and vector providers paginate deterministically across multiple pages with cryptographic cursor verification and pinned snapshot epochs.
  - id: OBL-0339-2
    package: WP-910
    proof: partitioned_hnsw_scales_beyond_65536_rows_and_enforces_lag_and_age_staleness
    description: PartitionedHnswIndex scales per-partition HNSW search beyond 65,536 rows across bounded <=65,536-row shards and enforces commit-lag and wall-clock age staleness thresholds alongside stale-entity count thresholds.
  - id: OBL-0339-3
    package: WP-910
    proof: vector_nearest_query_joins_related_entities_and_executes_hybrid_bm25_rrf
    description: Vector nearest queries join dependent related entities at a unified snapshot epoch and fuse with riff_bm25_v1 rankings via deterministic fixed-point integer Reciprocal Rank Fusion (hybrid_rrf_v1).
  - id: OBL-0339-4
    package: WP-911
    proof: tokenized_text_english_stemmer_and_bounded_fuzzy_matching_rank_deterministically
    description: Tokenized text supports deterministic Porter2 English stemming (english_stem_v1), bounded Levenshtein fuzzy term matching (edit distance <= 2) with fixed-point score attenuation, and UTF-8 bounded snippet extraction.
  - id: OBL-0339-5
    package: WP-911
    proof: long_pattern_linear_time_regex_prefilters_trigrams_and_extracts_snippets
    description: Long pattern search compiles regular expressions to a backtracking-free Thompson NFA with mandatory literal 3-gram prefiltering and UTF-8 bounded snippet extraction.
review_triggers:
  - "A multi-page cursor continuation crosses incompatible contract, module, plan, parameter, capability, or snapshot epochs without failing closed."
  - "A vector query ranks or builds HNSW shard topology before applying scalar predicates and row-level policy admission."
  - "A regular expression or fuzzy text query uses unbounded backtracking or floating-point relevance scoring."
---
# ADR-0339: Streaming Query Cursors, Multi-Segment Vector Joins and Hybrid RRF, and Stemmed/Fuzzy/Regex/Snippet Search

## Context

Three architectural limitations remained in `docs/known-limitations.md` under **Queries, Search, and Projections**:
1. **Single-page query surface:** While `symbolic_query.rs` minted `CursorToken` continuations for RiffQL, `riff_bm25_v1`, and `long_pattern_v1`, projected-table, exact-predicate, exact-index, and vector named queries rejected `cursor.is_some()`, and `riffdb-client-rust` lacked a first-class multi-page streaming cursor iterator that pins snapshot freshness across pages.
2. **Vector search ceiling, staleness, joins, and hybrid search:** Per-partition HNSW vector search capped `MAX_VECTOR_ANN_THRESHOLD_ROWS_PER_ORG` at `65,536` rows (`ann_cache` at `500` rows), supported only entity-count staleness (`staleness_slo <count>`), and required `program.steps().len() == 1` with no multi-source joins or single-query hybrid vector + BM25 execution.
3. **Text and pattern search expressiveness:** `riff_bm25_v1` lacked language stemming, bounded fuzzy matching, and snippets; `long_pattern_v1` lacked linear-time regular expressions and snippets.

## Decision

1. **Universal `CursorToken` Continuation & SDK Multi-Page Cursor Streams (`WP-909`):**
   - Extend 16-byte cryptographic `CursorToken` continuation (binding contract lineage/version, module hash, plan hash, parameter hash, capability ID/revision, and snapshot/provider epoch) to projected-table, exact-predicate, exact-index, and vector/hybrid named queries in `crates/riffdb-service/src/symbolic_query.rs`. Each individual page remains bounded at `65,534` rows and `4 MiB`.
   - Expose `NamedQueryPageStream` and `GeneratedQueryPageStream` (`stream_named_query` / `stream_generated_query` / `collect_pages`) in `crates/riffdb-client-rust`, automatically pinning `FreshnessRequirement::ReadAfterCommit(first_page.application_head)` and sticky endpoint routing across all pages.

2. **Multi-Segment Partitioned HNSW, Lag/Age Staleness, Vector Joins, and Hybrid RRF (`WP-910`):**
   - Raise `MAX_VECTOR_ANN_THRESHOLD_ROWS_PER_ORG` to `1,048,576` rows per partition by executing `PartitionedHnswIndex` across up to 16 bounded $\le 65,536$-row HNSW shards (`MAX_HNSW_SHARD_ROWS = 65,536`) after policy and scalar predicate admission (`VEC-007`, `VEC-008`), merging shard top-$k$ candidates deterministically.
   - Extend `VectorFieldSchema` and `VectorProjectionRequest` with optional `staleness_max_lag_commits` and `staleness_max_age_ms` alongside `staleness_slo` entity count (`VEC-004`).
   - Allow compiled vector queries to execute post-`nearest(...)` join/lookup/filter/project steps at the unified snapshot epoch and execute single-query hybrid `nearest(...)` + `riff_bm25_v1` ranking via deterministic integer Reciprocal Rank Fusion (`hybrid_rrf_v1`, `score_ug = vec_weight_bps * (1_000_000 / (60 + rank_vec)) + text_weight_bps * (1_000_000 / (60 + rank_bm25))`, tie-broken by canonical primary key ascending).

3. **Stemming, Bounded Fuzzy Matching, Linear-Time Regex, and Snippets (`WP-911`):**
   - Add `TextAnalyzerV1::EnglishStemV1 = 3` (`english_stem_v1`), performing Unicode 17.0.0 word segmentation, `unicode_fold_v1`, and deterministic Porter2 English suffix stripping.
   - Add bounded Levenshtein fuzzy term matching (`edit_distance` $\in \{1, 2\}$, `max_expansions` $\le 64$) in `tokenized_text` with deterministic fixed-point distance attenuation (`100%` exact, `75%` distance 1, `50%` distance 2).
   - Add `LongPatternOperatorV1::MatchesRegex = 9` (`matches_regex`), compiled to a backtracking-free Thompson NFA ($\le 256$ states, pattern $\le 1,024$ bytes) with mandatory literal 3-gram extraction for trigram postings prefiltering.
   - Provide UTF-8 codepoint-bounded snippet extraction (`extract_snippet_v1`, $\le 512$ bytes) for `riff_bm25_v1` and `long_pattern_v1`.

4. **Head-to-Head SaaS Query Benchmark vs PostgreSQL on C3D (`WP-912`):**
   - Extend `benchmarks/saas-query-bench` with multi-page cursor scan, stemmed/fuzzy BM25 + snippet, trigram regex + snippet, HNSW vector join, and hybrid vector + BM25 RRF workloads against PostgreSQL (`minimal` and `safe-app` profiles) and verify on C3D (`8 vCPU, AMD EPYC 9B14`).

## Options considered

1. **Unbounded single-RPC streaming or PCRE backtracking regexes:** rejected because unbounded streams and backtracking regexes violate RiffDB's fail-closed resource bounds.
2. **External vector/search sidecars:** rejected because external engines break atomic provenance, policy-before-ranking (`VEC-007`), and single-binary deployment.
3. **Snapshot-pinned multi-page cursor streams, sharded HNSW + RRF, Porter2 + bounded Levenshtein, and Thompson NFA + trigram prefilter:** chosen because every operation remains deterministic, policy-safe, and statically bounded.

## Consequences

- Multi-page queries stream deterministically across bounded pages with cryptographic cursor verification.
- Vector search scales to `1,048,576` rows/partition, enforces count/lag/age staleness, joins related entities, and fuses with BM25 via integer RRF.
- `riff_bm25_v1` and `long_pattern_v1` support English stemming, fuzzy matching, linear-time regexes, and snippets.

## Standing design tests

- **Interface safety:** Every cursor page, HNSW shard, fuzzy expansion, regex NFA state set, and snippet is statically bounded; policy admission always precedes ranking and scoring.
- **Scale:** Partitioned HNSW shards cap per-shard graph memory to 65,536 nodes; regex trigrams prefilter candidates before linear-time NFA evaluation.

## Checks

- Semantic unit and integration tests for `OBL-0339-1` through `OBL-0339-5` and C3D vs PostgreSQL benchmark execution (`WP-912`).
