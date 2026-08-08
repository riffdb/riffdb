# WP-481 constant-time query frontiers

Named-query execution no longer decodes the last retained command segment to
discover the current snapshot frontier. The query reads the application
sequence allocator in the same redb read transaction as authoritative entity
and index state, and checks command-authority presence in constant time.

The allocator remains authoritative because command effects, command
authority, and allocator advancement commit atomically. Empty, next, and
exhausted allocator states have explicit mappings. A gross disagreement
between allocator state and command-authority presence fails closed. Startup,
recovery, retention, projection scans, and explicit command-history reads still
decode and validate the complete requested segment; the hot query path is not a
replacement for those integrity proofs.

Warm query-module plans now use concurrent immutable cache reads. Cold misses
retain the bounded compile-and-publish path, exact module-hash identity, and
admission checks. A synchronized 4,096-lookup test proves that warm concurrent
lookups remain inline rather than falling back to the blocking worker pool.

## Public-path checkpoint

One 15-second diagnostic repetition per level, using the full TicketDesk seed
and per-session HTTP/2 connections, produced:

| clients | WP-480 | WP-481 | change | prior safe-app PostgreSQL | RiffDB / safe-app |
|---:|---:|---:|---:|---:|---:|
| 1 | 1,760 ops/s | 1,660 ops/s | -6% | 2,143 ops/s | 77% |
| 32 | 19,068 ops/s | 21,297 ops/s | +12% | 27,571 ops/s | 77% |
| 128 | 21,417 ops/s | 31,035 ops/s | +45% | 39,116 ops/s | 79% |

At c128, aggregate p50 fell from 5.24 ms to 0.82 ms and point-ticket p50
fell from 4.72 ms to 0.69 ms. Average named-plan lookup fell from 165.5 us to
52.3 us, while average query execution fell from 327.4 us to 34.5 us. At c32,
aggregate p50 fell from 0.75 ms to 0.34 ms and query execution from 297.6 us
to 36.8 us. Every measured operation completed without error.

The c1 checkpoint retained a 0.213--0.221 ms aggregate p50 and improved
average query execution from 71.3 us to approximately 40 us. Three repetitions
produced 1,699, 1,568, and 1,714 ops/s. The six-percent movement against
WP-480's single repetition did not carry a read- or write-latency regression,
but remains a known throughput limitation rather than being hidden by the
large concurrent gains.

Peak resident memory remains high: approximately 354 MiB at c1, 862 MiB at
c32, and 1.03 GiB at c128. WP-481 removes retained-history decoding and cache
contention; it does not claim to solve the process-memory footprint. The next
performance campaign should treat memory ownership and the increasingly
queued write lane as separate problems.

Artifacts:

- `target/app-baseline/wp-481-c1.json`
- `target/app-baseline/wp-481-c1-repeated.json`
- `target/app-baseline/wp-481-c32.json`
- `target/app-baseline/wp-481-c128.json`

These are local diagnostic artifacts rather than published benchmark claims.
