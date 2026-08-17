# WP-649 journal-fence mechanics gate

Date: 2026-08-17

Status: reject-first mechanics complete; production dispatch unchanged.

## Question

ADR-0132 asks whether a bounded append/fence pipeline or concurrent same-file
durability calls can remove the measured ordered-completion tail without
weakening RiffDB's gap-free durable and published frontiers.

The production journal currently drains every immediately ready submission,
writes its frames positionally into one pre-zeroed extent, and performs one
`fdatasync` for the complete ready prefix. The mechanics gate compares that
`coalesced_ready` control with:

- one `fdatasync` per logical group (`serial_per_group`);
- a bounded producer plus one coalescing durability worker
  (`pipelined_coalesced_fence`);
- persistent workers issuing concurrent writes and `fdatasync` calls against
  cloned handles for the same inode (`concurrent_same_file`); and
- the same workers using separate files (`concurrent_separate_files`).

Every file is completely zero-filled and fenced before measurement. Prefix
sizes are 4 KiB, 64 KiB, and 256 KiB. Depths are one, two, and four, with 128
iterations per cell. The probe uses real disk roots under `~/tmp/perf-db` and
refuses `/tmp`.

Activation required at least a 20% p95 improvement on both cloud profiles and
no more than a 5% throughput loss. No candidate passes.

## Receipts

| Profile | Host | vCPU | Raw receipt SHA-256 |
|---|---|---:|---|
| Workstation | local development host | 32 | `a90527563416054ea4e96f73ffb5839f8e384bee14d3787d7153d7cc4214b652` |
| N1 | `34.138.100.108` | 8 | `4c3612994ac044a90d41c37ee201d8b26e210d41de29eef97b79f43c960d5c5b` |
| E2 | `35.231.16.106` | 8 | `ba8df98c12d5ff578feef0ddfa0c806cbb42a1e7b68595d78cd367f8fc632253` |

The raw receipts remain in `/home/kevin/tmp/perf-db/wp649/` on the originating
hosts. They use schema `riffdb-journal-fence-pipeline-probe-v1`.

## Cloud comparison against ready-drain coalescing

Values are candidate changes versus `coalesced_ready`. Positive p95 is worse;
negative throughput is worse.

### N1

| Bytes | Depth | Candidate | p95 | Throughput |
|---:|---:|---|---:|---:|
| 4 KiB | 2 | pipelined/coalesced | +108% | -50% |
| 4 KiB | 2 | concurrent same file | +48% | -31% |
| 4 KiB | 4 | pipelined/coalesced | +73% | -46% |
| 4 KiB | 4 | concurrent same file | +77% | -38% |
| 64 KiB | 2 | pipelined/coalesced | +33% | -28% |
| 64 KiB | 2 | concurrent same file | +40% | -31% |
| 64 KiB | 4 | pipelined/coalesced | +39% | -30% |
| 64 KiB | 4 | concurrent same file | +41% | -22% |
| 256 KiB | 2 | pipelined/coalesced | +29% | -26% |
| 256 KiB | 2 | concurrent same file | +18% | -21% |
| 256 KiB | 4 | pipelined/coalesced | -11% | -15% |
| 256 KiB | 4 | concurrent same file | -5% | -15% |

### E2

| Bytes | Depth | Candidate | p95 | Throughput |
|---:|---:|---|---:|---:|
| 4 KiB | 2 | pipelined/coalesced | +44% | -33% |
| 4 KiB | 2 | concurrent same file | +40% | -28% |
| 4 KiB | 4 | pipelined/coalesced | +88% | -46% |
| 4 KiB | 4 | concurrent same file | +134% | -58% |
| 64 KiB | 2 | pipelined/coalesced | +48% | -43% |
| 64 KiB | 2 | concurrent same file | +22% | -29% |
| 64 KiB | 4 | pipelined/coalesced | +47% | -33% |
| 64 KiB | 4 | concurrent same file | +59% | -42% |
| 256 KiB | 2 | pipelined/coalesced | +45% | -36% |
| 256 KiB | 2 | concurrent same file | +30% | -30% |
| 256 KiB | 4 | pipelined/coalesced | -13% | -15% |
| 256 KiB | 4 | concurrent same file | -10% | -15% |

## Decision

Concurrent same-file durability and the tested append/fence pipeline are
rejected. The closest result improves p95 by only 13% and loses 15% throughput;
it misses both predeclared thresholds. The current ready-drain coalescing is the
best tested journal-media shape for the production frame range.

This falsifies the claim that the 6.6--24.7 ms ordered completion tail can be
removed merely by issuing more `fdatasync` calls. The physical journal lane is
not changed. WP-649 proceeds only with a closed production ledger separating
coordinator group residence, journal queue/write/sync, receipt readiness,
ordered publication, notification, and acknowledgement. Any later production
candidate must be sized from that ledger and requires exact ADR-0132 acceptance.
