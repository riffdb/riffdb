# WP-749 archive acknowledgement qualification (2026-10-15)

Direct same-binary archive disabled/enabled qualification pair on an isolated
8 vCPU AMD EPYC 9B14 host after eliminating archive collector I/O contention
and redundant receipt/frame hashing. Both cells pass host preflight/postflight,
workload correctness, raw-sample reconciliation, and archive-state checks with
zero errors, conflicts, sample drops, or checkpoint failures.

| Metric | Disabled (`01-disabled`) | Enabled (`02-enabled`) | Change |
| --- | ---: | ---: | ---: |
| Report `aggregate_p50_ns` (ms) | 5.767167 | 5.767167 | +0.00% |
| Report `aggregate_p95_ns` (ms) | 8.912895 | 8.912895 | +0.00% |
| Raw acknowledgement P50 (ms) | 5.658270 | 5.740610 | +1.46% (+82 µs) |
| Raw acknowledgement P95 (ms) | 8.453920 | 8.533410 | +0.94% (+79 µs) |
| Raw acknowledgement P99 (ms) | 14.131330 | 14.180380 | +0.35% (+49 µs) |
| Throughput (ops/s) | 5,056 | 4,997 | -1.17% |
| Measured successful requests | 455,130 | 449,808 | — |
| Terminal application archive lag | — | 0 (543,999 / 543,999) | — |

## Mechanisms addressed

1. **Reverse tail walk in `ChangelogSuffix::sources_from` and borrowed canonical inspection**:
   Opening or advancing a receipt cursor walks only the unarchived suffix tail (`O(delta)`) instead of cloning all `ChangelogSuffix` sources, and `CompositeMutationV1::inspect_canonical` validates borrowed slices without per-mutation `Vec<u8>` allocations.
2. **Single-pass receipt/frame digest computation and cached `AuthoritativeTransactionV3::history_hash`**:
   Precomputes `history_hash` once on `AuthoritativeTransactionV3` construction and computes `frame_checksum` and `file_digest` in one pass during `ChangelogFrameV3::encode_with_digests`, eliminating redundant multi-megabyte SHA-256 passes per frame.
3. **Batched immutable artifact sync in `RedbArchiveRepository::persist`**:
   Stages both the immutable frame and manifest files before issuing their retained descriptor syncs and directory sync barrier, preserving all crash recovery edges while eliminating an extra directory sync per frame.
4. **Yielding archive disk writes during background journal extent maintenance and timer-polled coalescing**:
   `riffdb_storage_redb::journal_extent_maintenance_active_v1` tracks active background checkpoint extent retirement/sealing and 320 MiB spare-extent zero-filling (`riffdb-journal-seal`). The archive worker defers disk writes during that ~0.85s zero-fill window, drains existing cursor frames before rebasing the snapshot pin, and coalesces publications on a 10 ms timer without registering per-commit `watch` wakeups on the foreground writer.
