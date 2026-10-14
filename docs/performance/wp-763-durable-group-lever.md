# WP-763 Durable-group Formation and Activation Gate

`WP-763` (`ADR-0183` Decisions 3 and 4) implements zero-wait idle-singleton sealing, prepared-prefix transition and byte ceiling enforcement (`256` transitions / `16 MiB`), `commands_per_durable_flush` shutdown evidence, and the `scripts/check-durable-group-activation` four-gate verifier across the `N1` (`gcp_n1_persistent_disk`) and `E2` (`gcp_e2_persistent_disk`) cloud profiles.

## 1. Durable-group Formation Mechanics (`ADR-0183` Decision 3)

| Mechanism | Implementation | Proof |
|---|---|---|
| Zero-wait idle singleton | When the writer is idle and draining the ready channel prefix leaves a single prepared command (`pending.len() == 1`), `CommandCoordinatorActor::run` sets `formation_deadline = None` and dispatches the singleton immediately without entering `collect_until_group_deadline` (`200 µs`). | `idle_singleton_seals_without_group_formation_wait` |
| Prepared-prefix transition and byte ceilings | `command_prefix_can_grow` and `form_next_unit` drain only the ready prepared command prefix in admission order, stopping with `CommitGroupDispatchReason::Full` at `MAX_GROUPED_WRITE_TRANSITIONS` (`256`) transitions or `MAX_STAGED_WRITE_BYTES` (`16 MiB`, `16,384` `QUEUED_COMMAND_BYTE_UNIT`s). `RedbDurabilityEpoch::seal` runs the `ADR-0129` frontier-equivalence proof once per sealed epoch. | `group_formation_drains_only_prepared_prefix_within_ceilings` |
| Shutdown flush-group census | `crates/riffdb-storage-redb` (`writer_durable_flush_group_census_v1`) and `crates/riffdb-server/src/shutdown_census.rs` emit `riffdb-durable-flush-group-v1` with 10 fixed-cardinality `commands_per_durable_flush` buckets (`1`, `2`, `3..=4`, `5..=8`, `9..=16`, `17..=32`, `33..=64`, `65..=128`, `129..=256`, `>256`), `epoch_queue_drained_seals`, and `epoch_ceiling_seals`. | `shutdown_evidence_reports_commands_per_durable_flush` |

## 2. Activation Gate Evaluation (`ADR-0183` Decision 4)

`scripts/check-durable-group-activation` evaluates the four `ADR-0183` section 4 activation gates against the `WP-762` baseline bank ([wp-762-baseline-bank.md](wp-762-baseline-bank.md)) and the `WP-674` low-water unary baseline:

1. Interactive `c32` throughput `>= 0.90x` on both `N1` and `E2`.
2. Interactive `c32` p95 `<= 1.25x` on both `N1` and `E2`.
3. `write-only` p95 `<= 1.05x` of the `WP-762` bank on both `N1` and `E2`.
4. Every unary scenario `p50` and `p95` `<= 1.10x` of the `WP-674` low-water baseline on both `N1` and `E2`.

| Profile | Banked `c32` Throughput Ratio | Gate (`>= 0.90x`) | Banked `c32` p95 Ratio | Gate (`<= 1.25x`) | Verdict |
|---|---:|---:|---:|---:|---|
| `N1` (`gcp_n1_persistent_disk`) | `0.705x` | Miss | `2.069x` | Miss | `miss-N1` |
| `E2` (`gcp_e2_persistent_disk`) | `0.687x` | Miss | `1.871x` | Miss | `miss-E2` |

### Why group-formation scheduling alone cannot clear the `0.90x` / `1.25x` gate

As established by the `WP-640` cloud writer attribution (`docs/performance/wp-627-cloud-writer-attribution.md`) and `WP-818` / `WP-819` / `WP-820`:

- At `c32` on 8-vCPU cloud hosts (`N1` and `E2`), the sole writer is already `99.8%` busy (`busy_us` vs `idle_us`), and ready prepared commands naturally accumulate in the intake channel while the prior durability epoch executes and seals (`mean batch size 4.18–5.85` at `c32`).
- The gap between `0.69–0.71x` (or `~0.80x` after `WP-787` removed fresh-locator coverage) and `0.90x` is bounded by per-command writer CPU (`~285 µs/cmd` on N1), not by ungrouped idle flushes or group-formation wait.
- Consequently, per `ADR-0183` Decision 4, no speculative cross-unit group-formation delay is activated (`activated: false`, `reasons: ["miss-N1", "miss-E2"]`). The zero-wait idle-singleton immediate path, `256`-transition / `16 MiB` prepared-prefix bounds, `commands_per_durable_flush` shutdown census, and `scripts/check-durable-group-activation` gate verifier remain active.

## 3. Verification

```bash
./scripts/check-durable-group-activation --self-test
cargo test -p riffdb-commit --all-features
cargo test -p riffdb-server --all-features
```
