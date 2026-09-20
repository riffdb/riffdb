# WP-791 derived sinks: local write-throughput measurement

Local confirmation that a contract declaring a projection no longer
contends the primary writer's exclusive mutation gate. Taken on this
workstation with `benchmarks/perf-surface`, 2,000 published documents,
32 clients, after ADR-0240's sidecar apply.

**Withdrawn.** The first local run did not assert that the projection
caught up (`GetProjectionStatus`). Those numbers are equally consistent
with the apply failing to see published journal-suffix commits, and must
not be quoted as evidence that declaring a projection is free.

Re-measure only after overlay-aware apply and a catch-up drain. This
page will record that run; it is not C3D confirmation.

Do not rewrite `docs/performance/perf-surface-mechanism-costs-2026-09.md`.
That page banks the diagnosis this package implements against.

## What changed

Event-derived projection apply writes a sibling redb engine
(`<database>.derived`). Commits and events that feed an apply are read
through a primary read transaction. The primary writer does not wait
for that read, and it never acquires the sidecar's write transaction.

Columnar control records stay on the primary gate. They are
`ReplicatedAuthoritative` rows that retention still reads from the
primary store (`retention.rs` is a guarantee-tier surface owned outside
this package). Their cadence is per ingest batch, not per document; the
measured 80% write-throughput cost was event-derived projection apply,
and idling that apply restored baseline. Leaving columnar controls on
the gate is the scoped alternative ADR-0240 permits when cadence makes
them immaterial to the write path under test.

## Local perf-surface, 32 clients, 2000 documents

Three consecutive runs of `benchmarks/perf-surface` on this workstation,
2026-09-19, `riffdbd` built from this tree. Throughput is publish-phase
wall time (`documents / elapsed`), not `writer_busy_us`.

| rep | base docs/s | projection docs/s | projection vs that run's base |
|---|---:|---:|---:|
| 1 | 6,445 | 7,364 | **+14.3%** |
| 2 | 7,194 | 6,895 | **-4.2%** |
| 3 | 6,820 | 7,212 | **+5.8%** |

Base range 6,445–7,194 (mean 6,820, spread 11.0% of mean). Projection
range 6,895–7,364 (mean 7,157). Every projection cell is inside the
base run-to-run band except the first, which is faster than base, not
slower. The diagnosis this package implements against was an 80% loss
(1,830 vs 5,350 docs/s on C3D); that coupling is gone on this host.

`writer_busy_us` does not tile wall clock. Busy versus publish elapsed
on these runs:

| variant | rep | elapsed s | busy s | remainder |
|---|---|---:|---:|---:|
| base | 1 | 0.310 | 0.267 | 0.043 (14%) |
| base | 2 | 0.278 | 0.243 | 0.035 (13%) |
| base | 3 | 0.293 | 0.238 | 0.055 (19%) |
| projection | 1 | 0.272 | 0.250 | 0.022 (8%) |
| projection | 2 | 0.290 | 0.275 | 0.015 (5%) |
| projection | 3 | 0.277 | 0.248 | 0.029 (10%) |

Busy is 81–95% of elapsed here and does not explain the remaining wall
time. Throughput is the claim; the busy column is corroboration only.
