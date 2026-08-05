# WP-457 Writer-Feeding Decision

WP-457 tested whether allowing more generated-client work in flight would feed
the sole writer well enough to reach the three-second full-seed target. The
answer was no: the server formed larger physical groups and issued fewer
durable flushes, but serialized validation, encoding, and staging cost grew
faster than the saved commit cost.

The retained public policy is:

- Rust, TypeScript, and Python generated command batches accept concurrency
  from 1 through 128 and reject zero or 129 before submission.
- Each Rust transport exchange still carries at most 16 ordinary commands, so
  concurrency 128 permits at most eight simultaneous exchanges.
- The production coordinator admission queue defaults to 512 independently
  byte-bounded messages. This is an internal queue bound, not an application
  transaction or public tuning control.
- The physical group remains dynamically selected from 1 through the static
  256-command safety ceiling.

## Candidate sweep

All measurements used the same release build and the full 19,220-command seed
over the public gRPC application surface. Postgres was skipped. Each point is a
single diagnostic run, so it supports the bounded retain-or-revert decision but
is not stable release-comparison evidence.

| Requested concurrency | Seed | Commands/s | Completion groups | Mean group | Largest group | Durable flush total | Validation/encoding/staging | `create_comment` p50 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 128 | 3.767 s | 5,102 | 342 | 56.30 | 112 | 2.409 s | 0.978 s | 2.077 ms |
| 192 | 5.096 s | 3,772 | 252 | 76.41 | 187 | 2.660 s | 1.587 s | 2.156 ms |
| 256 | 3.966 s | 4,846 | 195 | 98.75 | 244 | 2.147 s | 1.313 s | 2.803 ms |

Every dispatch was `QueueDrained`; no count saturation, overload, semantic
failure, conflict-key split, or exact-access split occurred. Larger client
concurrency therefore does feed larger groups, but that is not the limiting
optimization. The 192 candidate is 35% slower than 128. The 256 candidate is 5%
slower and regresses the representative unary mutation by 35%.

The 192/256 public expansion is rejected. TypeScript still rises from its old
32-item maximum to the shared 128-item maximum. The server's 512-message queue
is retained because it restores the required independent admission headroom
above one maximum physical group without changing command semantics.

## Next performance lever

Do not raise client concurrency again before reducing per-group CPU. The next
work should profile and reduce validation/encoding/staging amplification,
especially repeated protobuf construction, mutation-graph clones, and
transaction-local rereads. A later candidate must beat the 128 point on total
seed time and preserve unary latency; reducing physical commit count alone is
not sufficient.
