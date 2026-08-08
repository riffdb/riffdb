# WP-480 journal mechanics

The preallocated-journal format is gated by same-filesystem mechanics evidence.
On 2026-08-07, 96 measured fences per scenario produced:

| mechanism | best p50 |
|---|---:|
| extending append + fdatasync | 4,479 us |
| sparse positional + fdatasync | 4,452 us |
| pre-zeroed positional + fdatasync | 880 us |
| pre-zeroed positional + O_DSYNC | 880 us |

Fully zero-filled positional writes are 5.09x faster. Sparse reservation does
not avoid first-write metadata cost, and O_DSYNC does not beat explicit
fdatasync. Production therefore selects fully written extents, positional
writes, and explicit fences.

## Public-path checkpoint

One 15-second diagnostic repetition per level, using the unchanged full
TicketDesk seed and per-session HTTP/2 connections, produced:

| clients | before WP-480 | after WP-480 | change | prior safe-app PostgreSQL | RiffDB / safe-app |
|---:|---:|---:|---:|---:|---:|
| 1 | 1,464 ops/s | 1,761 ops/s | +20% | 2,143 ops/s | 82% |
| 8 | 5,559 ops/s | 9,569 ops/s | +72% | 10,063 ops/s | 95% |
| 32 | 13,256 ops/s | 19,068 ops/s | +44% | 27,571 ops/s | 69% |
| 128 | 26,337 ops/s | 21,417 ops/s | -19% | 39,116 ops/s | 55% |

Write p50 moved from 8.9 to 3.54 ms at c8 and from 15.2 to 6.82 ms
at c32. The 19,220-command seed completed in 2.71--2.81 seconds at
6,845--7,082 commands/s, meeting the sub-three-second target.

At c1, write p50 fell from 2.6 to 1.70 ms and throughput rose 20 percent.
Aggregate p50 was 0.221 ms versus the earlier 0.183 ms, while point reads
remained 0.188 ms and every operation completed without error.

The c128 regression is not a durability-fence ceiling: aggregate read p50
rose to 4.72 ms and named-query execute time dominated, while c8/c32 write
latency improved sharply. The next campaign therefore belongs to concurrent
read execution, retained-history footprint, and per-command CPU/redb apply;
the extending-file journal mechanism is no longer the primary limiter.

Artifacts:

- `target/app-baseline/wp-480-c8.json`
- `target/app-baseline/wp-480-c32.json`
- `target/app-baseline/wp-480-c128.json`
- `target/app-baseline/wp-480-c1.json`

```text
cargo +1.97.0 run --release \
  --manifest-path benchmarks/journal-mechanics/Cargo.toml
```

The default probe root is `target/perf-db/journal-mechanics`; it never selects
an operating-system temporary directory.
