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

## Steady-state mid-seed CPU finding

An initial six-second capture was rejected because its samples included
projected-query execution and the graceful-shutdown checkpoint. A second run
used a 64,700-command seed and captured ten seconds while seed progress was
actively advancing. Startup and shutdown were outside the capture.

| Thread class | CPU samples | Dominant work |
| --- | ---: | --- |
| authoritative command coordinator | 38.13% | transaction-local group drive, capsule application, composite validation, segment insertion |
| rebuildable columnar worker | 30.86% | bounded commit scans, composite range merge, command-segment decode, projection apply |
| asynchronous journal checkpoint | 15.31% | frame decode/validation, command-segment decode, redb materialization |
| service runtime workers | 13.43% | command application orchestration and transport |
| journal fence worker | 1.60% | physical extent write and sync |
| idempotency inspection | 0.59% | bounded inspection |

The authoritative writer is not consuming the whole machine. Almost half of
steady-state seed CPU is concurrent catch-up/materialization of state that is
derived from or already represented by the durable journal. The next candidate
must therefore reduce duplicate commit scanning/decoding or coalesce derived
catch-up under write pressure while preserving exact frontiers, typed
freshness, bounded journal capacity, restart replay, and eventual catch-up.
Another isolated encoder or coordinator micro-optimization is not authorized
by this evidence.

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

## Documentation impact

None. The shutdown census and this report are maintainer-only performance
evidence. They add no public option, metric label, protocol field, durability
choice, or application-visible behavior.
