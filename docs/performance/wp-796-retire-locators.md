# WP-796 — Retire Durable Command-Derived Locator Tables (ADR-0244)

- **Date:** 2026-10-14
- **Package:** `WP-796`
- **Decision Record:** `ADR-0244`
- **Base Revision:** `3b851020a` (`main` after `WP-815` / `ADR-0263`)
- **Host Specification:** `8 vCPU, AMD EPYC 7B12`, ext4 on network persistent disk

## Summary

Per `ADR-0244`, `WP-796` retires the three durable command-derived locator tables introduced by `ADR-0165` (`idempotency_locators`, `outbox_command_locators`, and `provenance_locators`, namespaces `39`, `40`, and `41`) from the write path, checkpoint application, and fresh table creation, and serves command-derived record resolution through the `ADR-0102` in-memory `CommandDerivedIndex` (`IdempotencyKey -> CommitSequence`, `EventId -> CommitSequence`, `CommitId -> CommitSequence`).

To preserve `ADR-0165`'s fail-closed corruption discipline without durable locator tables:

1. **Startup Completeness Before Readiness:** Bounded clean startup loads a verified `CommandDerivedIndex` snapshot when its frontier matches the durable commit frontier, or performs a bounded scan of retained `COMMITS` up to `MAX_STARTUP_COMMAND_DERIVED_REBUILD_SCAN_ROWS` (`65,536` rows) before the store becomes ready. If the retained commit table exceeds the bounded scan ceiling without a valid snapshot, startup refuses with `StorageError::CorruptedData` rather than serving from an incomplete index or scanning unbounded history.
2. **Fail-Closed Locator Resolution (`OBL-0244-1`):** Every lookup through the ready `CommandDerivedIndex` (`command_derived_locator`, `idempotency_record`, `outbox_entry_by_id`, `provenance_by_commit_id`, and `service_audit_sequences_for`) treats a missing or mismatched target command segment, or a missing locator on an unwarmed bounded-startup handle, as typed `StorageError::CorruptedData` and never as `Ok(None)`. This is proven by `a_missing_or_mismatched_locator_is_corruption_not_absence`.
3. **Backward Compatibility for Pre-ADR-0244 Fixtures:** `AuthoritativeNamespaceV1::is_retired_locator_table` classifies namespaces `39..=41` as retired locator tables so pre-`ADR-0244` durable fixtures reopen cleanly while ignoring legacy locator tables during inventory and recovery walks.

## Methodology

A counterbalanced ABBA live-server A/B comparison (`base-1`, `cand-1`, `cand-2`, `base-2`, 3 repetitions per arm, 6 repetitions per build, 12 runs total) was executed on the `8 vCPU, AMD EPYC 7B12` bench host using `./benchmarks/run-app-baseline --full --load write_only --load-clients 32 --load-duration-secs 30 --load-warmup-secs 5 --skip-postgres`.

- **Base binary (`riffdbd`):** SHA-256 `2007eef493d1043791ffb019f4c5079b0d8089ed5f6e2236f3f734adbd2b05cc`
- **Candidate binary (`riffdbd`):** SHA-256 `70cc9fcc38a1ece9bc574f20c3781f3a8a5e56c831be60982a1f1a8f0ff94613`

## Per-Repetition Results

| Arm / Rep | Build | Seed Throughput (ops/s) | Seed Journal Bytes | Write-Only Throughput (ops/s) | Write-Only p50 (ms) | Write-Only p95 (ms) | Write-Only p99 (ms) | Write-Only Max (ms) | Load Journal Bytes / Cmd | Checkpoints (Rows / Bytes) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `base-1` r1 | Base | 3,957.2 | 91,591,582 | 2,229 | 14.156 | 19.923 | 23.069 | 151.826 | 5,544.4 | 3 (72,856 / 403.8 MB) |
| `base-1` r2 | Base | 4,175.5 | 91,591,366 | 2,278 | 13.631 | 18.874 | 22.020 | 160.057 | 5,554.4 | 3 (72,731 / 403.9 MB) |
| `base-1` r3 | Base | 4,149.4 | 91,580,388 | 2,238 | 14.156 | 19.923 | 23.069 | 155.795 | 5,557.7 | 3 (72,813 / 404.5 MB) |
| `cand-1` r1 | Candidate | 4,344.5 | 83,255,541 | 2,245 | 14.156 | 19.923 | 23.069 | 116.336 | 5,109.9 | 2 (52,827 / 270.5 MB) |
| `cand-1` r2 | Candidate | 4,180.1 | 83,290,491 | 2,197 | 14.680 | 19.923 | 24.117 | 101.419 | 5,118.3 | 2 (52,534 / 269.6 MB) |
| `cand-1` r3 | Candidate | 4,191.0 | 83,272,596 | 2,266 | 14.156 | 19.923 | 23.069 | 98.093 | 5,123.2 | 2 (52,573 / 270.0 MB) |
| `cand-2` r1 | Candidate | 4,306.5 | 83,269,042 | 2,257 | 14.156 | 19.923 | 23.069 | 99.992 | 5,110.7 | 2 (52,555 / 269.2 MB) |
| `cand-2` r2 | Candidate | 4,178.3 | 83,271,121 | 2,147 | 14.680 | 20.972 | 25.166 | 103.545 | 5,119.1 | 2 (52,384 / 268.8 MB) |
| `cand-2` r3 | Candidate | 4,151.2 | 83,266,575 | 2,282 | 14.156 | 18.874 | 23.069 | 103.822 | 5,123.5 | 2 (52,401 / 269.0 MB) |
| `base-2` r1 | Base | 3,693.3 | 91,584,199 | 2,030 | 15.204 | 20.972 | 25.166 | 175.414 | 5,542.8 | 2 (48,605 / 270.2 MB) |
| `base-2` r2 | Base | 3,874.2 | 91,582,156 | 2,004 | 15.729 | 22.020 | 27.263 | 174.185 | 5,556.2 | 2 (48,440 / 269.9 MB) |
| `base-2` r3 | Base | 4,038.7 | 91,577,312 | 2,099 | 15.204 | 20.972 | 24.117 | 166.452 | 5,555.6 | 2 (48,200 / 268.7 MB) |

## Aggregate Comparison

| Metric | Base (6 reps) | Candidate (6 reps) | Candidate / Base Ratio (or Delta) |
| --- | --- | --- | --- |
| **Seed Throughput (geomean)** | `3,977.9 ops/s` (range `3,693.3–4,175.5`, spread `1.1306`) | `4,224.6 ops/s` (range `4,151.2–4,344.5`, spread `1.0466`) | **`1.0620x`** (`+6.20%`) |
| **Seed Journal Bytes (19,220 cmds)** | `91,584,500 B` (`4,765.1 B/cmd`) | `83,270,894 B` (`4,332.5 B/cmd`) | **`-432.5 B/cmd`** (`-9.08%`, `-8.31 MB`) |
| **Write-Only Throughput (geomean)** | `2,143.7 ops/s` (range `2,004–2,278`, spread `1.1367`) | `2,231.8 ops/s` (range `2,147–2,282`, spread `1.0629`) | **`1.0411x`** (`+4.11%`) |
| **Write-Only Journal Bytes / Cmd** | `5,551.9 B/cmd` | `5,117.5 B/cmd` | **`-434.4 B/cmd`** (`-7.82%`) |
| **Write-Only Max Latency (geomean)** | `163.72 ms` (range `151.83–175.41 ms`) | `103.71 ms` (range `98.09–116.34 ms`) | **`0.6335x`** (`-36.65%`) |
| **Publication B-Tree Stage (`publication_stages_v2` col 2 max)** | `91.28–119.48 ms` | `40.46–64.34 ms` | **`-46% to -56%` max stall** |
