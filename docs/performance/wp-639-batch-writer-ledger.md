# WP-639 batch writer ledger and stronger-candidate bounds

WP-639 measures the full generated-command seed path at one exact boundary and
designs, but does not implement, the next coordinator candidate. The
`--seed-only` diagnostic shuts the public app-baseline process down immediately
after the 19,220-command seed and retains the existing fixed-cardinality writer
evidence in JSON. It does not change the frozen `PERF-018` comparator or produce
release evidence.

## Closed seed ledger

Both runs use revision `48b917c9`, standard durability, 128 generated-client
seed workers, the full TicketDesk seed, and the public gRPC application path.
Writer busy plus preceding idle closes to the client-measured seed wall within
two percent.

| measure | workstation | N1 |
|---|---:|---:|
| seed wall | 2,808.5 ms | 11,149.8 ms |
| writer busy | 2,437.0 ms | 9,458.1 ms |
| writer idle | 416.9 ms | 1,814.1 ms |
| busy + idle | 2,853.9 ms | 11,272.2 ms |
| closure error | +1.62% | +1.10% |
| commands | 19,220 | 19,220 |
| physical groups | 340 | 353 |
| mean commands/group | 56.53 | 54.45 |

The disjoint coordinator costs are:

| mean per command | workstation | N1 | current classification |
|---|---:|---:|---|
| admission | 5.90 us | 16.35 us | admission ordered |
| compatibility/conflict ownership | 1.09 us | 3.96 us | sequencing essential |
| deterministic evaluation | 16.84 us | 68.78 us | movable under accepted PERF-008 text |
| validation, encoding, staging | 52.70 us | 201.58 us | mixed; requires a proof split |
| final authoritative apply | 41.82 us | 157.91 us | sequencing essential, but not irreducibly this expensive |
| journal sealing after apply | 2.96 us | 24.83 us | ordered, group-amortizable |
| publication | 0.02 us | 0.09 us | post-fence ordering essential |
| unattributed writer-loop remainder | 5.48 us | 18.59 us | no movement credited |
| **writer busy** | **126.80 us** | **492.10 us** | closed sum |
| writer idle | 21.69 us | 94.39 us | feeding/fence/drain overlap |

Fence duration is not added to writer busy because durability is pipelined.
The summed deferred-completion duration is 111.71 us/command on the workstation
and 454.24 us/command on N1, or 6.32 ms and 24.73 ms per group. Actual physical
flush work is only 24.10 and 25.89 us/command respectively. Mean complete frame
size is 155.8 KiB and 150.1 KiB. The N1 gap between 1.41 ms mean physical I/O and
24.73 ms mean ordered completion is retained for the eventual write-p95 work;
this package does not alter group or fence policy.

## What the seed ledger rules out

Moving evaluation plus the complete validation/staging bucket, an impossible
best case because part of that bucket is transaction-current, leaves at least
57.3 us/command of workstation writer service and 221.7 us/command on N1 before
idle or preparation cost. Those floors imply about 1.10 seconds and 4.26 seconds
for 19,220 commands. The current safe-application PostgreSQL targets are about
0.501 seconds on the workstation and 1.586 seconds on N1, making the 1.10x gates
0.551 seconds and 1.745 seconds.

Coordinator preparation alone therefore cannot meet seed parity. The stronger
candidate must also make final apply consume already-proven immutable batch
material without reconstructing, decoding, hashing, or revalidating the same
graph per command. ADR-0129 defines that candidate and keeps final current-state
checks, conflict ownership, sequence assignment, ordered apply, fence, and
publication authoritative on the coordinator path.

## All-gate sizing

The candidate is intentionally rejectable before a semantic rewrite:

| gate | current | release target | candidate requirement |
|---|---:|---:|---:|
| workstation mixed c32 | 29,360/s, 0.76x | at least 34,716/s | at least +18.2% |
| N1 mixed c32 | 6,675/s, 0.77x | at least 7,805/s | at least +16.9% |
| workstation seed | 2.808 s, 5.61x | at most 0.551 s | at least 5.10x |
| N1 seed | 11.150 s, 7.03x | at most 1.745 s | at least 6.39x |
| unary writes | 1.26--1.71x on cloud | at most 1.10x | no claimed gain; no more than 5% regression |

For seed reachability, the N1 bounded preparation pool must reduce the current
270.36 us evaluation-plus-validation CPU charge to at most 180 us/command and
process it on at most three workers, while admission-ordered finalization and
apply must fall below 60 us/command. On the 16-core workstation, the ordered
path must fall below 25 us/command. These targets make the effective preparation,
serial, and physical-fence lanes each no slower than roughly 60--75 us/command
on N1 and 25--29 us/command locally, leaving bounded headroom for the public seed
gate. Failure of either mechanics threshold rejects the candidate.

At N1 c32, reducing the measured 0.908 ms serialized write service to 0.35 ms
or less raises the writer ceiling from about 1,100 to 2,850 writes/s. At the
interactive mix's 15% write share, the 7,805 ops/s release target requires only
1,171 writes/s and about 41% writer utilization at that service time. Queueing
should therefore cease to dominate; the candidate gate is the public ratio,
not the internal prediction. Workstation c32 must independently reach its
0.90x target.

Unary c1 latency is not improved by moving work to a pool and may gain another
handoff. ADR-0127 transport remainder and separately measured pre-fence work
retain ownership after the coordinator result. WP-640 may not claim release
completion while any unary operation remains above 1.10x.

## Comparator-history reconciliation

ADR-0123's workstation statement used run `tri-20260815T155726Z`; WP-638 uses
`tri-20260816T135850Z`. Between ADR-0123 and WP-638, the safe-application SQL,
operation weights, PostgreSQL 18.4 image and durability configuration, host-
network loopback topology, and runner were byte-identical. RiffDB c32 changed
only 28,102 to 29,360 ops/s (+4.5%), while safe PostgreSQL changed 23,621 to
38,573 ops/s (+63.3%). The older PostgreSQL c32 repetitions were 6,310, 29,211,
and 35,341 ops/s, a 5.60x spread that the current stability gate rejects; the
new repetitions were 36,588, 38,952, and 40,178, a 1.10x spread. The maintainer
subsequently identified and stopped unrelated `next-server` processes on the
workstation. The historical ratio change is therefore host-interference and
unstable-comparator drift, not a workload, PostgreSQL, topology, or RiffDB
semantic change. The older mean is retained as history but is not a target.

## Receipts

| receipt | SHA-256 |
|---|---|
| workstation seed-only | `4c24e535d34c7481537cddafb870519889b95cb490f7e3b3fe34340c71717f4e` |
| N1 seed-only | `2918e31fdf924b5ee1743220df6fc80c353dd2c113ea8d28eb66e69ded6bd0e0` |
| old workstation safe PG | `4d49eb40e0e2a32e8a9c0e351c097a6f81e9452f479229bc13c6d8f3ac144729` |
| old workstation RiffDB | `a0b7fe786f90cc2ca3f6db1c680fac2b4f8f61999fe4d3cd63787bddfcb1d918` |
| current workstation safe PG | `3d99d61734082b54e2731a87e76c547e3fad18b32f5e6930fea40298685db057` |
| current workstation RiffDB | `aa2f0e70b090568f235025acd3fd46950f37f476658f6b8c721d66e1b9db209e` |

