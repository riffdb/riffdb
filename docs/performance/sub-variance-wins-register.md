# Sub-variance wins register

A change can be real and still be too small to measure. The C3D bench host's
run-to-run variance is 2 to 4 percent, and ADR-0239 decision 3 refuses a
single-run delta smaller than that as a result. Taken literally on each change
in isolation, that rule would refuse every improvement under about 3 percent
forever — which in a database, at scale, is most of them.

This register is how those changes get taken anyway without weakening the rule.
An entry is **implemented and merged** on its own evidence, and its system-level
effect is **measured in aggregate** with the others rather than claimed alone.

## What qualifies

1. **Measurable where it is real.** The win is demonstrated at the level it
   occurs — a primitive benchmark, an algorithmic complexity change, a removed
   allocation — with numbers, not an argument.
2. **Behaviour-preserving, and proven so.** Byte-identical output, or identical
   observable state, with a test that fails if that stops being true. The test
   is the deliverable, not the speedup.
3. **No system-level claim.** Nothing here is banked, quoted in a release note,
   or used to justify relaxing a reviewed boundary, until the batch is measured
   together on the bench host against the previous banked revision.

## Open entries

| change | measured effect | estimated share of writer busy | evidence |
|---|---|---|---|
| Hardware CRC-32C for envelope checksums | 5.54 → 9.52 GB/s at the 6,393-byte frame size, 1.72x, 482 ns saved per call | ~0.34% (encode and decode) | `crc_matches_the_software_table_across_sizes_and_alignments` |
| Tokenized-text paging clones only the returned page | removes a key clone and a `CanonicalRecord` clone per candidate outside the page | dormant; no contract declares the provider | `paging_returns_the_same_order_and_rows_as_one_unpaged_page` |
| `long_pattern` incremental postings | O(N^2) → O(N); 250/500/1000 inserts at 5.25/11.09/23.61 ms | dormant; no contract declares the provider | `incremental_maintenance_equals_a_full_rebuild_over_the_same_rows` |
| Derived sidecar absence observed once per handle (ADR-0282) | `statx` per read 1.0 → 0.000 on the second development machine (8 vCPU, AMD EPYC 7B12), c32 and c1 read-only; read throughput and CPU per read within that host's spread | ~1.6% of tokio-worker CPU in the c32 read-only C3D profile | `absent_sidecar_is_probed_once_and_a_later_creation_is_read` |
| Query-execute diagnostic flag read from the environment once per process | `getenv` and its environment-lock frames 0.21% → 0 of daemon samples, write-only c32 on the second development machine; the lookup ran on every journal and checkpoint point read on the command and idempotency threads | ~0.2% of daemon CPU under write-only | `the_diagnostic_flag_is_read_from_the_environment_at_most_once` |
| Trailers share the DATA frame's write (h2 v0.4.20 plus hyperium/h2#980, pinned in the workspace `Cargo.toml`) | `writev` per read 1.97–2.00 → 0.92–0.97 at c32 and 1.94–2.17 → 0.97–1.17 at c1; tokio-worker CPU per read 325 → 288 µs median at c32 (−11%) and 506 → 486 µs at c1 (−4%, the three rounds taken with the host otherwise idle) on the second development machine, read-only, alternating arms; throughput within that host's spread | the removed `writev` per gRPC response with a body of 256 bytes or more | h2's `headers_after_a_chained_data_frame_share_its_write` and `server_sends_large_data_then_trailers_in_order` |
| `exact_text` (V1/V2/V3) incremental sorted postings & zero-alloc checkpoint bound check | O(N^2 log N) → O(N log N); 250/500/1000 single-row epochs 24,185/109,086/510,569 ms → 392.3/1,093.6/3,772.8 ms (61.7x/99.8x/135.3x) on the second development machine (8 vCPU, AMD EPYC 7B12) | dormant unless projection declares exact-text index | `filtered_text_incremental_replacement_removal_and_nulls_match_full_rebuild`, `randomized_incremental_writes_match_independent_truth_table` |
| `exact_predicate` (V4/V5) incremental order & sparse field maintenance | O(N^2) → O(N); 250/500/1000 single-row epochs 159.8/663.2/3,087.6 ms → 6.15/32.41/131.06 ms (26.0x/20.5x/23.6x) on the second development machine (8 vCPU, AMD EPYC 7B12) | dormant unless projection declares exact-predicate index | `incremental_epoch_maintenance_equals_full_rebuild_over_the_same_rows` |
| `tokenized_text` incremental postings & zero-alloc checkpoint bound check | O(N^2) → O(N); 250/500/1000 single-row epochs 148.3/555.9/2,558.7 ms → 4.58/15.94/71.23 ms (32.4x/34.9x/35.9x) on the second development machine (8 vCPU, AMD EPYC 7B12) | dormant unless contract declares `text_index` | `incremental_epoch_maintenance_equals_full_rebuild_over_the_same_rows` |
| `TokenizedTextPartitionIndexV1::total_field_length` maintained incrementally | O(D) per-query scan over all documents → O(log F) `field_totals` lookup | dormant unless contract declares `text_index` | `total_field_length_tracks_upserts_updates_deletes_compaction_and_checkpoints` |
| Row-policy relationship lookup memoization within one pinned read snapshot | O(rows × lookups) index range scans → O(distinct lookups) within `StorageQueryView`, `RedbQueryView`, `authorize_candidates`, `authorize_event_candidates`, and `authorize_provider_projected_candidates_admitted_v1` (20 rows sharing 2 relationships: 20 → 2 range scans) | dormant unless contract declares a row policy | `projected_candidate_admission_deduplicates_identical_relationship_lookups` |

## Deferred, not yet taken

| candidate | why deferred |
|---|---|
| `chunks_exact_to_as_chunks` (72 sites, Rust 1.98 lint) | Deferred by ADR-0241 so the toolchain bump measured a compiler change alone. Unblocked now; unmeasured. |
| Cross-query `CorpusStatistics` caching by provider epoch | Recorded in `deferred-dormant-path-optimizations.md`; requires trait-level epoch caching across queries on a path no contract declares today. |

## When to measure the batch

When the estimated aggregate share reaches roughly 3 percent — the point at
which one C3D run against the previous banked revision can distinguish it from
noise. Measure once, bank once, under ADR-0239's discipline. If the aggregate
does not show up, that is the result, and it is worth knowing: it would mean the
per-primitive numbers are not reaching the system, which is the same failure the
2026-09 capture attribution nearly shipped a durable-format change on.

## What this register is not

It is not permission to skip measurement. Every entry carries a real number at
the level it was taken. It is a refusal to let "too small to measure alone"
become "never worth doing", while still refusing to claim what has not been seen.
