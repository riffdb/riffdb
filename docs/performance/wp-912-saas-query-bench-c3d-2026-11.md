# WP-912 SaaS Complex Query, Search, Cursor, and Vector Benchmark on C3D (2026-11)

Head-to-head benchmark comparing RiffDB against PostgreSQL (`minimal` and
`safe-app` profiles) on the C3D benchmark host (`8 vCPU, AMD EPYC 9B14`) using
`./benchmarks/run-saas-query-bench --full`.

## Benchmark Configuration

- **Hardware specification:** `8 vCPU, AMD EPYC 9B14` (NUMA 1 node, C3D standard profile)
- **Harness:** `benchmarks/saas-query-bench` (`--full` scale)
- **Pre-timing exact parity checks:**
  - `20 / 20` SaaS analytics & aggregation parity checks (`riffdbd == postgres-minimal == postgres-safe-app`)
  - `6 / 6` `ADR-0333` `mutate any` pool claim & `ADR-0334` compiled durable saga parity checks
  - `5 / 5` `ADR-0339` multi-page streaming cursor, stemmed/fuzzy BM25 with snippets, trigram regex with snippets, `PartitionedHnswIndex` vector join, and hybrid vector + BM25 RRF parity checks

## 1. Unary Streaming Cursors, Stemmed/Fuzzy BM25, Trigram Regex, Vector Joins & Hybrid RRF (`ADR-0339`, `c = 1`, microseconds — lower is better)

| Workload | RiffDB Construct | Engine | p50 (µs) | p95 (µs) | p99 (µs) | Mean (µs) | QPS |
|---|---|---|---:|---:|---:|---:|---:|
| `multi_page_cursor_scan` | Snapshot-pinned `CursorToken` 4-page stream (32 rows/page, 128 rows total) | RiffDB | 57 | 66 | 75 | 58 | 16950 |
| `multi_page_cursor_scan` | Snapshot-pinned `CursorToken` 4-page stream (32 rows/page, 128 rows total) | PostgreSQL (`minimal`) | 563 | 595 | 609 | 491 | 2031 |
| `multi_page_cursor_scan` | Snapshot-pinned `CursorToken` 4-page stream (32 rows/page, 128 rows total) | PostgreSQL (`safe-app`) | 517 | 545 | 628 | 518 | 1927 |
| `stemmed_fuzzy_bm25_with_snippets` | `english_stem_v1` + Levenshtein fuzzy (`d <= 1`) + `extract_snippet_v1` | RiffDB | 112 | 121 | 123 | 113 | 8753 |
| `stemmed_fuzzy_bm25_with_snippets` | `english_stem_v1` + Levenshtein fuzzy (`d <= 1`) + `extract_snippet_v1` | PostgreSQL (`minimal`) | 597 | 627 | 640 | 598 | 1669 |
| `stemmed_fuzzy_bm25_with_snippets` | `english_stem_v1` + Levenshtein fuzzy (`d <= 1`) + `extract_snippet_v1` | PostgreSQL (`safe-app`) | 746 | 786 | 799 | 749 | 1333 |
| `trigram_regex_with_snippets` | `long_pattern_v1` `MatchesRegex` (3-gram prefilter + Thompson NFA) + snippet | RiffDB | 56 | 64 | 66 | 57 | 17320 |
| `trigram_regex_with_snippets` | `long_pattern_v1` `MatchesRegex` (3-gram prefilter + Thompson NFA) + snippet | PostgreSQL (`minimal`) | 298 | 332 | 355 | 303 | 3294 |
| `trigram_regex_with_snippets` | `long_pattern_v1` `MatchesRegex` (3-gram prefilter + Thompson NFA) + snippet | PostgreSQL (`safe-app`) | 431 | 475 | 502 | 436 | 2285 |
| `hnsw_vector_multi_source_join` | `PartitionedHnswIndex` `nearest(k=10)` + related `Project` entity join | RiffDB | 59 | 67 | 68 | 59 | 16701 |
| `hnsw_vector_multi_source_join` | `PartitionedHnswIndex` `nearest(k=10)` + related `Project` entity join | PostgreSQL (`minimal`) | 120 | 138 | 148 | 122 | 8119 |
| `hnsw_vector_multi_source_join` | `PartitionedHnswIndex` `nearest(k=10)` + related `Project` entity join | PostgreSQL (`safe-app`) | 242 | 271 | 283 | 247 | 4037 |
| `hybrid_vector_bm25_rrf` | Single-query `PartitionedHnswIndex` + `riff_bm25_v1` integer RRF (`k=60`) | RiffDB | 57 | 64 | 66 | 57 | 17241 |
| `hybrid_vector_bm25_rrf` | Single-query `PartitionedHnswIndex` + `riff_bm25_v1` integer RRF (`k=60`) | PostgreSQL (`minimal`) | 129 | 150 | 154 | 132 | 7528 |
| `hybrid_vector_bm25_rrf` | Single-query `PartitionedHnswIndex` + `riff_bm25_v1` integer RRF (`k=60`) | PostgreSQL (`safe-app`) | 254 | 279 | 293 | 258 | 3861 |

## 2. Concurrent `ADR-0339` Search, Cursor, and Vector Mix Throughput (higher QPS / lower µs is better)

| Workload | Concurrency | Engine | QPS | p50 (µs) | p95 (µs) | p99 (µs) | Mean (µs) |
|---|---:|---|---:|---:|---:|---:|---:|
| `adr_0339_search_cursor_vector_mix` | `1` | RiffDB | 12554 | 67 | 125 | 136 | 79 |
| `adr_0339_search_cursor_vector_mix` | `1` | PostgreSQL (`minimal`) | 2770 | 331 | 630 | 670 | 360 |
| `adr_0339_search_cursor_vector_mix` | `1` | PostgreSQL (`safe-app`) | 2129 | 455 | 748 | 788 | 469 |
| `adr_0339_search_cursor_vector_mix` | `8` | RiffDB | 49779 | 97 | 207 | 219 | 125 |
| `adr_0339_search_cursor_vector_mix` | `8` | PostgreSQL (`minimal`) | 10800 | 670 | 1487 | 2108 | 704 |
| `adr_0339_search_cursor_vector_mix` | `8` | PostgreSQL (`safe-app`) | 7574 | 972 | 1762 | 2161 | 1007 |
| `adr_0339_search_cursor_vector_mix` | `32` | RiffDB | 101535 | 98 | 211 | 2242 | 164 |
| `adr_0339_search_cursor_vector_mix` | `32` | PostgreSQL (`minimal`) | 14046 | 1652 | 4912 | 6656 | 2038 |
| `adr_0339_search_cursor_vector_mix` | `32` | PostgreSQL (`safe-app`) | 10479 | 2530 | 5309 | 7918 | 2806 |

## 3. SaaS Complex Query, Pool Claim, and Durable Saga Baseline (`--full` on `8 vCPU, AMD EPYC 9B14`)

| Workload | Concurrency | RiffDB QPS / Ops/s | PostgreSQL (`minimal`) | PostgreSQL (`safe-app`) |
|---|---:|---:|---:|---:|
| `candidate_intersection_and_distinct_hydration` | `1` | 2975 | 4549 | 1816 |
| `candidate_intersection_and_distinct_hydration` | `8` | 14214 | 14246 | 7697 |
| `sla_quantiles_dispersion_and_tags` | `8` | 5564 | 8820 | 7490 |
| `saas_analytics_mix` | `8` | 4901 | 9370 | 5803 |
| `pool_claim_mutate_any` | `8` | 3147 | 8243 | 3907 |
| `saga_checkout_forward` | `1` | 181 | 360 | 139 |
| `saga_checkout_compensate` | `1` | 133 | 235 | 91 |
