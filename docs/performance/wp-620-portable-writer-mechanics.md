# WP-620 Portable Writer and Checkpoint Mechanics

WP-620 records diagnostic mechanics for the two hardware classes required by
ADR-0123. These measurements are not the `PERF-018` qualification and cannot
waive correctness, durability, or the final PostgreSQL comparison.

## Profiles

| Profile | CPU | Storage/filesystem | Revision |
| --- | --- | --- | --- |
| workstation | AMD Ryzen 9 7950X, 16 cores/32 threads | local NVMe, ext4 | `dfae57d8` |
| cloud | GCP general-purpose VM, Intel Xeon 2.30 GHz, 4 cores/8 threads | persistent `/dev/sda1`, ext4 | `dfae57d8` |

Both runs used Rust 1.97.0, release builds, a clean benchmark root outside
tmpfs, the production 40-MiB pre-zeroed extent geometry, and the same bounded
workloads. Raw JSONL receipts are retained outside the repository under
`~/tmp/riffdb-wp620-{local,cloud}` on their respective hosts.

## Journal fence size sweep

Pre-zeroed positional `fdatasync` p50, in microseconds:

| Logical bytes | Workstation | Cloud |
| ---: | ---: | ---: |
| 3,000 | 964 | 431--453 |
| 65,536 | 873--889 | 571--616 |
| 262,144 | 883--892 | 799--888 |
| 1,048,576 | 1,001--1,055 | 3,086--3,149 |
| 4,194,304 | 1,692--1,722 | 14,267--14,279 |
| 16,777,216 | 4,262--4,275 | 57,112--57,395 |

The cloud disk is faster for tiny fences but scales much more sharply with
bytes. That explains why a small unary mutation can look healthy while a large
high-concurrency completion group loses badly. It also makes encoded-byte
reduction a portable optimization rather than a workstation-only CPU tweak.

## Authoritative table census

The stable 1,024-command mixed workload produced a 4,845,320-byte journal
suffix on both profiles. Its logical mutation values were:

| Table | Mutations | Value bytes | Key bytes | Fixed V1 header bytes |
| --- | ---: | ---: | ---: | ---: |
| command segments (`commits`) | 8 | 3,670,016 | 128 | 352 |
| entities | 1,024 | 786,432 | 16,384 | 45,056 |
| secondary indexes | 1,024 | 262,144 | 16,384 | 45,056 |
| index epochs | 8 | 1,024 | 128 | 352 |
| metadata | 8 | 64 | 200 | 352 |

Command segments are about 76 percent of the complete frame. All keys and
fixed mutation headers together are about two percent. This falsifies the
original prefix-key-first proposal and establishes the compact command segment
selected by ADR-0123 as the only candidate in this campaign capable of a large
gain.

## CPU and checkpoint costs

Median mixed-workload observations:

| Commands | Stage | Workstation | Cloud |
| ---: | --- | ---: | ---: |
| 1,024 | current frame build + redb apply | 7.19 ms | 48.75 ms |
| 1,024 | overlay frame build + publish | 2.89 ms | 35.79 ms |
| 1,024 | checkpoint transaction | 12.59 ms | 18.25 ms |
| 4,096 | current frame build + redb apply | 25.80 ms | 195.22 ms |
| 4,096 | overlay frame build + publish | 15.59 ms | 157.09 ms |
| 4,096 | checkpoint transaction | 27.64 ms | 74.85 ms |

The 1,024-command checkpoint writes approximately 6.21 MiB and the 4,096
checkpoint approximately 24.71 MiB. Checkpoint cadence is already bounded and
correct; the evidence does not justify postponing checkpoint work, raising the
suffix ceiling, or replacing redb. It justifies reducing the bytes that both
the foreground frame and the checkpoint carry.

## Repeat

```bash
RIFFDB_JOURNAL_MECHANICS_ROOT="$HOME/tmp/riffdb-wp620/mechanics" \
  cargo +1.97.0 run --release \
  --manifest-path benchmarks/journal-mechanics/Cargo.toml

cargo +1.97.0 run --release \
  --manifest-path benchmarks/command-growth/Cargo.toml \
  --bin journal-state-overlay -- \
  --database-root "$HOME/tmp/riffdb-wp620/overlay" --reps 3
```

The old `journal-state-overlay` decision gate still reports the abandoned
WP-486 two-times overlay-speedup hypothesis. That field is retained for schema
continuity; it is not WP-620's exit gate and must not be relabeled as passing.
WP-620's falsifiable result is the complete size/cost census above.

## Next gate

WP-621 must use a captured real TicketDesk command corpus in addition to this
synthetic bounded mechanics corpus. Activation requires all ADR-0123 size,
cloud writer-stage, and unary non-regression thresholds. WP-623 then repeats
the full PostgreSQL comparison on both profiles before the 72-hour soak.
