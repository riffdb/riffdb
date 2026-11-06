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

Each workload is measured across both RiffDB's compiled named-query plan (`RiffDB (compiled plan)`) and the public application client (`RiffDB (public client gRPC)` via `StableApplicationClient` over live `riffdbd` with role capability verification, parameter encoding, and response decoding), alongside PostgreSQL `minimal` and `safe-app` profiles.

| Workload | RiffDB Construct | Engine | p50 (µs) | p95 (µs) | p99 (µs) | Mean (µs) | QPS |
|---|---|---|---:|---:|---:|---:|---:|
| `multi_page_cursor_scan` | Snapshot-pinned `CursorToken` 4-page stream (32 rows/page, 128 rows total) | RiffDB (compiled plan) | 58 | 67 | 72 | 59 | 16732 |
| `multi_page_cursor_scan` | Snapshot-pinned `CursorToken` 4-page stream (32 rows/page, 128 rows total) | RiffDB (public client gRPC) | 1272 | 1316 | 1340 | 1253 | 797 |
| `multi_page_cursor_scan` | Snapshot-pinned `CursorToken` 4-page stream (32 rows/page, 128 rows total) | PostgreSQL (`minimal`) | 443 | 468 | 487 | 445 | 2243 |
| `multi_page_cursor_scan` | Snapshot-pinned `CursorToken` 4-page stream (32 rows/page, 128 rows total) | PostgreSQL (`safe-app`) | 571 | 598 | 617 | 565 | 1767 |
| `stemmed_fuzzy_bm25_with_snippets` | `english_stem_v1` + Levenshtein fuzzy (`d <= 1`) + `riff_bm25_snippets_v1` | RiffDB (compiled plan) | 119 | 136 | 147 | 121 | 8211 |
| `stemmed_fuzzy_bm25_with_snippets` | `english_stem_v1` + Levenshtein fuzzy (`d <= 1`) + `riff_bm25_snippets_v1` | RiffDB (public client gRPC) | 327 | 354 | 367 | 328 | 3041 |
| `stemmed_fuzzy_bm25_with_snippets` | `english_stem_v1` + Levenshtein fuzzy (`d <= 1`) + `riff_bm25_snippets_v1` | PostgreSQL (`minimal`) | 632 | 663 | 681 | 632 | 1578 |
| `stemmed_fuzzy_bm25_with_snippets` | `english_stem_v1` + Levenshtein fuzzy (`d <= 1`) + `riff_bm25_snippets_v1` | PostgreSQL (`safe-app`) | 779 | 823 | 860 | 780 | 1279 |
| `trigram_regex_with_snippets` | `long_pattern_v1` `matches_regex` (3-gram prefilter + Thompson NFA) + snippet | RiffDB (compiled plan) | 56 | 66 | 75 | 57 | 17242 |
| `trigram_regex_with_snippets` | `long_pattern_v1` `matches_regex` (3-gram prefilter + Thompson NFA) + snippet | RiffDB (public client gRPC) | 684 | 730 | 746 | 683 | 1461 |
| `trigram_regex_with_snippets` | `long_pattern_v1` `matches_regex` (3-gram prefilter + Thompson NFA) + snippet | PostgreSQL (`minimal`) | 344 | 390 | 412 | 352 | 2830 |
| `trigram_regex_with_snippets` | `long_pattern_v1` `matches_regex` (3-gram prefilter + Thompson NFA) + snippet | PostgreSQL (`safe-app`) | 499 | 549 | 570 | 500 | 1997 |
| `hnsw_vector_multi_source_join` | `PartitionedHnswIndex` `nearest(k=10)` + related `Project` entity join | RiffDB (compiled plan) | 56 | 64 | 69 | 56 | 17436 |
| `hnsw_vector_multi_source_join` | `PartitionedHnswIndex` `nearest(k=10)` + related `Project` entity join | RiffDB (public client gRPC) | 599 | 695 | 711 | 612 | 1630 |
| `hnsw_vector_multi_source_join` | `PartitionedHnswIndex` `nearest(k=10)` + related `Project` entity join | PostgreSQL (`minimal`) | 155 | 173 | 178 | 158 | 6293 |
| `hnsw_vector_multi_source_join` | `PartitionedHnswIndex` `nearest(k=10)` + related `Project` entity join | PostgreSQL (`safe-app`) | 286 | 316 | 326 | 291 | 3426 |
| `hybrid_vector_bm25_rrf` | Single-query `hybrid_rrf_snippets_v1(embedding, $query_vector)` integer RRF (`k=60`) | RiffDB (compiled plan) | 122 | 136 | 150 | 124 | 8007 |
| `hybrid_vector_bm25_rrf` | Single-query `hybrid_rrf_snippets_v1(embedding, $query_vector)` integer RRF (`k=60`) | RiffDB (public client gRPC) | 349 | 383 | 403 | 352 | 2834 |
| `hybrid_vector_bm25_rrf` | Single-query `hybrid_rrf_snippets_v1(embedding, $query_vector)` integer RRF (`k=60`) | PostgreSQL (`minimal`) | 168 | 188 | 199 | 171 | 5821 |
| `hybrid_vector_bm25_rrf` | Single-query `hybrid_rrf_snippets_v1(embedding, $query_vector)` integer RRF (`k=60`) | PostgreSQL (`safe-app`) | 295 | 330 | 342 | 301 | 3314 |

## 2. Concurrent `ADR-0339` Search, Cursor, and Vector Mix Throughput (higher QPS / lower µs is better)

| Workload | Concurrency | Engine | QPS | p50 (µs) | p95 (µs) | p99 (µs) | Mean (µs) |
|---|---:|---|---:|---:|---:|---:|---:|
| `adr_0339_search_cursor_vector_mix` | `1` | RiffDB (compiled plan) | 10277 | 76 | 142 | 151 | 96 |
| `adr_0339_public_client_grpc_mix` | `1` | RiffDB (public client gRPC) | 1576 | 604 | 1206 | 1236 | 633 |
| `adr_0339_search_cursor_vector_mix` | `1` | PostgreSQL (`minimal`) | 2971 | 327 | 612 | 648 | 336 |
| `adr_0339_search_cursor_vector_mix` | `1` | PostgreSQL (`safe-app`) | 2063 | 473 | 771 | 814 | 484 |
| `adr_0339_search_cursor_vector_mix` | `8` | RiffDB (compiled plan) | 52784 | 115 | 224 | 237 | 148 |
| `adr_0339_public_client_grpc_mix` | `8` | RiffDB (public client gRPC) | 6457 | 1092 | 2330 | 2729 | 1219 |
| `adr_0339_search_cursor_vector_mix` | `8` | PostgreSQL (`minimal`) | 10692 | 633 | 1510 | 2010 | 704 |
| `adr_0339_search_cursor_vector_mix` | `8` | PostgreSQL (`safe-app`) | 7647 | 954 | 1868 | 2314 | 1003 |
| `adr_0339_search_cursor_vector_mix` | `32` | RiffDB (compiled plan) | 60247 | 112 | 232 | 6232 | 307 |
| `adr_0339_public_client_grpc_mix` | `32` | RiffDB (public client gRPC) | 8162 | 3185 | 8503 | 9819 | 3854 |
| `adr_0339_search_cursor_vector_mix` | `32` | PostgreSQL (`minimal`) | 14211 | 1657 | 4811 | 6855 | 2037 |
| `adr_0339_search_cursor_vector_mix` | `32` | PostgreSQL (`safe-app`) | 10393 | 2530 | 5586 | 8070 | 2786 |

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
