# WP-627 Cloud Writer Attribution

WP-627 added one fixed-cardinality, process-generation census at the physical
journal fence. It counts successful command-bearing physical flushes, command
frames, logical commands, complete frame bytes, maximum frames per flush, and
cumulative/maximum write-plus-sync time. The counters carry no application
identity, key, value, symbol, or per-command label and do not change durable
bytes or command behavior.

The diagnostic ran on the isolated GCP persistent-disk host used by WP-621
through WP-626. These are short attribution cells, not PERF-018 release
evidence.

## Physical durability finding

| Cell | Public throughput | Command frames | Physical flushes | Frames / flush | Physical I/O |
| --- | ---: | ---: | ---: | ---: | ---: |
| c=1, 15 s | 983 ops/s | 2,689 | 2,689 | 1.000 | earlier five-counter census |
| c=32, 15 s | 6,739 ops/s | 2,613 | 2,609 | 1.002 | earlier five-counter census |
| c=32, 10 s | 6,735 ops/s | 1,720 | 1,716 | 1.002 | 1.828 s total; 1.065 ms mean; 15.522 ms max |
| full seed | 19,220 commands / 11.291 s | 356 | 356 | 1.000 | 0.489 s total; 1.373 ms mean; 6.972 ms max |

The journal worker's drain path is real, but production feeds it almost
exactly one already-grouped command frame at a time. More importantly, the
measured physical write-plus-sync time is only about 16.6% of c=32 writer-busy
time and 4.3% of full-seed wall time. Perfect co-fencing cannot by itself close
the cloud gap and therefore fails WP-627's ten-percent-headroom rule for the
seed.

The c=32 writer stage sums were 0.410 s admission, 0.055 s compatibility,
1.656 s evaluation, 4.373 s validation/encoding/staging, and 0.006 s
publication. Full-seed sums were 0.321 s, 0.080 s, 1.242 s, 3.950 s, and
0.002 s respectively. These are overlapping stage totals, not a substitute for
process CPU attribution.

The 15.522 ms maximum flush is consistent with checkpoint/filesystem tail
interference, but the fixed-cardinality census carries no timestamps and cannot
prove that correlation. Any future checkpoint-path candidate must add bounded
temporal correlation before attributing or claiming removal of this tail.

## Steady-state mid-seed CPU finding

An initial six-second capture was rejected because its samples included
projected-query execution and the graceful-shutdown checkpoint. A second run
used a 64,700-command seed and captured ten seconds while seed progress was
actively advancing. Startup and shutdown were outside the capture.

| Thread class | CPU samples | Core-seconds | Dominant work |
| --- | ---: | ---: | --- |
| authoritative command coordinator | 38.13% | 9.08 | transaction-local group drive, capsule application, composite validation, segment insertion |
| rebuildable columnar worker | 30.86% | 7.35 | bounded commit scans, composite range merge, command-segment decode, projection apply |
| asynchronous journal checkpoint | 15.31% | 3.64 | frame decode/validation, command-segment decode, redb materialization |
| service runtime workers | 13.43% | 3.20 | command application orchestration and transport |
| journal fence worker | 1.60% | 0.38 | physical extent write and sync |
| idempotency inspection | 0.59% | 0.14 | bounded inspection |

The capture contained 23.804 core-seconds in a 10-second window on an
eight-vCPU host: 29.8% of machine capacity, not CPU saturation. The sample
shares prove that derived work exists, but do not prove that it delays the
authoritative critical path. In particular, batching catch-up could increase
typed freshness waits and is not authorized from this seed-only evidence.

## Low-concurrency freshness fork

The ordinary `interactive` profile invokes authoritative named RiffQL reads;
it does not invoke `ExecuteProjectedQuery`. Projected board reads use
`Available` freshness in separate scenarios after the pre-timing causal gate.
Therefore the c=1 and c=8 cells do not wait for columnar freshness at all.
Their `execute` stage is authoritative snapshot/query execution, not hidden
catch-up wait time.

| Host / cell | Throughput | aggregate p50 | read execute mean | all recorded read stages mean |
| --- | ---: | ---: | ---: | ---: |
| N1 c=1 | 1,011 ops/s | 0.655 ms | 100.4 us | 189.3 us |
| N1 c=8 | 4,834 ops/s | 0.950 ms | 128.6 us | 229.4 us |
| E2 c=1 | 756 ops/s | 0.918 ms | 118.1 us | 225.4 us |
| E2 c=8 | 5,359 ops/s | 0.918 ms | 116.9 us | 195.1 us |

The gap between client-observed latency and the sum of server stages is large
at c=1 on both CPU families. The next low-concurrency investigation belongs at
the generated-client/runtime/HTTP2 boundary and in finer decomposition of the
authoritative `execute` stage, not in projection catch-up.

## Matched interactive c=32 CPU finding

Ten-second `task-clock` captures were triggered only after the full seed and
when the server crossed 1.5 cores of work in the actual mixed c=32 cell. The
profiled public result and the capture are from the same process generation.

| Host | Profiled throughput | Core-seconds / 10 s | Machine utilization | service/Tokio | coordinator | columnar | checkpoint |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| N1 Intel, 8 vCPU | 5,803 ops/s | 44.416 | 55.5% | 55.64% / 24.72 core-s | 19.19% / 8.52 core-s | 16.79% / 7.46 core-s | 6.44% / 2.86 core-s |
| E2 AMD, 8 vCPU | 7,482 ops/s | 41.429 | 51.7% | 56.60% / 23.45 core-s | 18.53% / 7.68 core-s | 15.80% / 6.55 core-s | 7.25% / 3.00 core-s |

The matched workload is service-runtime dominated on both hosts and still has
substantial idle CPU. The unprofiled E2 c=32 cell reached 8,471 ops/s. The AMD
host's SHA extensions reduce CPU per operation and improve the medium/high
concurrency cells, but c=1 remains slower than N1. That cross-host shape again
points to serialized per-operation latency at low concurrency rather than a
whole-machine CPU ceiling.

## Comparator durability and cross-host ratios

The retained N1 tri-backend report and the fresh E2 server both report
`synchronous_commit=on`, `fsync=on`, `full_page_writes=on`, and
`wal_sync_method=fdatasync`. The E2 comparison used PostgreSQL 18 with 200
connections so the c=128 cell was admitted rather than silently clamped.

| Host / cell | RiffDB | PostgreSQL safe-app | RiffDB / safe-app |
| --- | ---: | ---: | ---: |
| N1 c=1 | 1,011 | 1,726 (three-rep mean) | 0.59x |
| N1 c=8 | 4,834 | 8,330 (three-rep mean) | 0.58x |
| N1 c=32 | 6,525 unprofiled | 8,815 (three-rep mean) | 0.74x |
| E2 c=1 | 756 | 1,286 | 0.59x |
| E2 c=8 | 5,359 | 8,882 | 0.60x |
| E2 c=32 | 8,471 unprofiled | 10,648 | 0.80x |

These are short diagnostic cells. They establish that the c=1/c=8 failure is
hardware-independent even though E2 improves the scaling knee.

## Predeclared candidate arithmetic

The previously suggested duplicate command-segment decode candidate is not
authorized by the corrected evidence:

| Cell | Hard Amdahl bound | Predicted public gain before implementation | Decision |
| --- | --- | ---: | --- |
| full seed | even deleting the entire 15.31% checkpoint class is only `1 / (1 - 0.1531) = 1.181x`; actual decoder samples are a small subset | 0-4% | reject |
| interactive c=1 | derived consumers are off the authoritative read path and the host has spare cores | 0-1% | reject |
| interactive c=32 | deleting the entire 6.44% N1 checkpoint class is only `1.069x`; duplicate decode is a subset and CPU is 55.5% utilized | 0-2% | reject |

For the next *measurement* candidate, the exposed fraction is the client-visible
time outside recorded server stages. At c=1 this is approximately 466 us on N1
and 692 us on E2 when comparing aggregate p50 with mean stage sums; the unlike
statistics make those attribution estimates, not additive latency proofs. The
next package must split generated-client assembly, synchronous-to-async runtime
entry, tonic request/response work, and finer authoritative execution. Before
that package changes an execution path, its declared activation targets are:

- seed: 0% expected (the seed already uses the asynchronous batch path);
- c=1: 20-35% expected, with at least 15% required;
- c=8: 10-20% expected, with at least 10% required;
- c=32: 0-8% expected; no regression is allowed.

One extra service-job spawn cannot satisfy this gate by itself: measured
`spawn_dispatch` is only about 14-20 us/read. Any candidate must remove a
larger demonstrated handoff or allocation chain rather than weakening
supervision, cancellation containment, authorization safe points, or the public
gRPC boundary.

## Evidence receipts

- c=1: `~/tmp/wp627-cloud/c1.json`, SHA-256
  `522b3c9d29178790f7d4d8823dc991c59a3d65839363f89232530da0732cafe5`.
- c=32 five-counter cell: `~/tmp/wp627-cloud/c32.json`, SHA-256
  `0ed71ea074ae75861373896f50cfd8bfa81e910765aaf2b069fcc4fd10775eb8`.
- c=32 timed-I/O cell: `~/tmp/wp627-cloud/c32-io.json`, SHA-256
  `7fc11ccc7710c522bacaf06917601a80960beef3234a176179102ad8989f6b76`.
- rejected teardown-contaminated profile:
  `~/tmp/wp627-seed.data`, SHA-256
  `a971d1c21956e4721c57072cff06788e85997b68fd17edcd181f76c306a27b97`.
- accepted active-seed profile:
  `~/tmp/wp627-midseed-1786838535694/midseed.data`, SHA-256
  `82a597dfbc6e10e223ce54b37d513c9b01b6b35fd609c26aca13b12943a13a8e`.
- accepted active-seed run report:
  `~/tmp/wp627-midseed-1786838535694/report.json`, SHA-256
  `58fc8deb4d53192a2595a195bd8b1227196bdc9510e55ef3500b3aeb879fdf27`.
- N1 c=1 freshness-fork report:
  `~/tmp/wp627-fork-n1-c1/report.json`, SHA-256
  `734c67def08ef16ad7f8918885c4c02e4fc8d916164f6584e0fa2f4adf255d0f`.
- N1 c=8 freshness-fork report:
  `~/tmp/wp627-fork-n1-c8/report.json`, SHA-256
  `8c851b81f0c8d0d52454803a4077ff68aaab9ba30a3d70f6f20e21fa823961d6`.
- N1 matched c=32 report and perf data: SHA-256
  `59948845d8678c254192f95c37ade587cba564994ec42fca1cb169f00c3c78d1`
  and `23eeaff292e707728eae11bdbbfe0a87b62ef5e150d7d894aa6f816c3542e8b9`.
- E2 c=1 and c=8 reports: SHA-256
  `b022299685ff981e0a7a13d659a060cf8d8b6d4ad4dae44af513564c6f5b71fe`
  and `eaa59ea97228793a6cb52e0a20c152add3418f68e80fdc072c64272c4673d41d`.
- E2 matched c=32 report and perf data: SHA-256
  `d0139613be5fefa7b9a0ec9b2e44e2257fdf0e7f28a23167a9139b0a36387df6`
  and `5436f4c8351d2e305564cbd684c61d2014095b72df32c3edf2b62054f751ddb1`.
- E2 unprofiled c=32 report: SHA-256
  `9772ca658861d2728d4198f34f6bdb3d84baf66886472ffd6f4d05717fc39874`.
- E2 safe-app and minimal PostgreSQL sweep reports: SHA-256
  `86f26fb3c5643efb80333222f1e280d2b8c0f6bcbd49a497449ba08d0b948eb5`
  and `af1f238a1b20e9f7eca6857fb76807c6f966fa785b231d247c0b4e3f35c5c861`.

## Documentation impact

None. The shutdown census and this report are maintainer-only performance
evidence. They add no public option, metric label, protocol field, durability
choice, or application-visible behavior.
