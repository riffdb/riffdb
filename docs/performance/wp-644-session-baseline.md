# WP-644 bounded-session current-HEAD baseline

This note freezes the short diagnostic baseline used to size ADR-0127's
bounded multiplexed application-session candidate. It is not PERF-018 release
evidence: the mixed cells use two 10-second repetitions after two seconds of
warmup, and the dedicated cells use two repetitions of 100 measured samples.

Both hosts ran the same current product source after WP-641 and WP-642. WP-643
changes only the recovery test controller, so commit `bf8df0ac` is
performance-equivalent. PostgreSQL used the harness-managed safe-application
profile over host-network loopback. Every measured operation completed with
zero errors, conflicts, unavailable outcomes, or idempotency mismatches.

## Mixed interactive baseline

| host | clients | safe PG ops/s | RiffDB ops/s | RiffDB / PG | PG p95 | RiffDB p95 | p95 ratio | PG create p50 | RiffDB create p50 | write ratio |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| N1 | 1 | 1,526 | 897 | 0.59x | 2.36 ms | 3.34 ms | 1.42x | 2.23 ms | 3.21 ms | 1.44x |
| N1 | 8 | 7,856 | 4,809 | 0.61x | 3.54 ms | 6.03 ms | 1.70x | 3.28 ms | 5.24 ms | 1.60x |
| N1 | 32 | 8,211 | 7,223 | 0.88x | 15.20 ms | 20.97 ms | 1.38x | 12.85 ms | 18.87 ms | 1.47x |
| N1 | 128 | 7,118 | 7,707 | 1.08x | 73.40 ms | 96.47 ms | 1.31x | 61.87 ms | 83.89 ms | 1.36x |
| E2 | 1 | 621 | 585 | 0.94x | 6.29 ms | 5.11 ms | 0.81x | 5.24 ms | 4.72 ms | 0.90x |
| E2 | 8 | 6,298 | 4,244 | 0.67x | 4.98 ms | 6.68 ms | 1.34x | 4.33 ms | 5.90 ms | 1.36x |
| E2 | 32 | 7,650 | 6,717 | 0.88x | 17.04 ms | 20.97 ms | 1.23x | 14.16 ms | 18.35 ms | 1.30x |
| E2 | 128 | 6,901 | 6,648 | 0.96x | 79.69 ms | 104.86 ms | 1.32x | 67.11 ms | 92.27 ms | 1.38x |

E2's two short c1 safe-PostgreSQL repetitions were 785 and 456 ops/s. The
mean is retained honestly but is not a stability claim. Candidate cells must
remain paired and the release decision still requires PERF-018's stable
90-second repetitions.

## Dedicated unary and seed baseline

| scenario | N1 PG p50 | N1 RiffDB p50 | ratio | E2 PG p50 | E2 RiffDB p50 | ratio |
|---|---:|---:|---:|---:|---:|---:|
| point ticket | 0.406 ms | 0.803 ms | 1.98x | 1.102 ms | 1.398 ms | 1.27x |
| point user | 0.292 ms | 0.671 ms | 2.30x | 0.838 ms | 1.289 ms | 1.54x |
| tickets by project/status | 0.347 ms | 0.869 ms | 2.51x | 0.685 ms | 1.498 ms | 2.19x |
| open tickets by assignee | 0.447 ms | 1.162 ms | 2.60x | 0.725 ms | 1.788 ms | 2.47x |
| comments for ticket | 0.340 ms | 0.728 ms | 2.14x | 0.882 ms | 1.331 ms | 1.51x |
| project members | 0.318 ms | 0.703 ms | 2.21x | 0.795 ms | 1.284 ms | 1.61x |
| ticket detail | 0.642 ms | 0.899 ms | 1.40x | 1.381 ms | 1.511 ms | 1.09x |
| board 50 | 1.865 ms | 2.007 ms | 1.08x | 2.007 ms | 2.500 ms | 1.25x |
| board 200 | 4.633 ms | 4.386 ms | 0.95x | 4.173 ms | 4.695 ms | 1.13x |
| board 450 | 2.983 ms | 8.371 ms | 2.81x | 3.227 ms | 8.132 ms | 2.52x |
| create comment | 2.703 ms | 3.772 ms | 1.40x | 5.915 ms | 5.830 ms | 0.99x |
| close with comment | 2.683 ms | 3.553 ms | 1.32x | 6.317 ms | 5.765 ms | 0.91x |
| swap member roles | 2.354 ms | 3.193 ms | 1.36x | 5.263 ms | 5.214 ms | 0.99x |
| open with labels | 2.541 ms | 3.780 ms | 1.49x | 6.074 ms | 5.962 ms | 0.98x |

N1 seed is 4.78x same-run PostgreSQL at the stable median, leaving 4.6 percent
headroom under PERF-008's 5.0x ceiling. E2 seed is 2.00x. The session candidate
does not replace the bounded batch seed transport, so any seed movement is a
regression until separately attributed.

## Predeclared decision arithmetic

The candidate must remove customer-paid per-unary HTTP/2/service orchestration,
not the already-falsified 6--9 microsecond synchronous benchmark bridge. Its
activation gates are the exact ADR-0127 gates: at least +15 percent c1 and +10
percent c8 versus same-run unary RiffDB; c32 at least 0.90x PostgreSQL
throughput and at most 1.25x PostgreSQL p95; every dedicated unary scenario at
most 1.10x PostgreSQL; seed at most 5.0x PostgreSQL; and no otherwise-uncovered
metric regression above five percent.

On N1, the minimum c32 changes are +2.3 percent throughput and -9.4 percent
RiffDB p95. On E2 they are +2.5 percent throughput while p95 is already inside
the ratio. N1 dedicated writes need 16.9--26.1 percent lower latency to reach
1.10x. E2 dedicated writes already pass, so they are strict no-regression
cells. The larger named-read misses mean a session may prove worthwhile yet
still fail release activation; the package must report that result rather than
silently narrowing the representative set.

## Receipts

| receipt | SHA-256 |
|---|---|
| N1 mixed report | `8204968a236e983451b115ebf505c2968e326471dda7ce0da97ab8846f5d4a7f` |
| N1 mixed log | `d4cfea605377c0058a73e429218364ce7a43249d881ff950a032808a638ead50` |
| E2 mixed report | `793e47da7610641a182a5085cd026f575d3215d0195ac09f1df06ca61e154efa` |
| E2 mixed log | `1eddda625d8a5eb77f64120f51ca94fc286c7922bef09ed1e00966a0065574fc` |
| N1 unary report | `42215e5d23e202601580c1b9a5e8d2c5039a58c61546668e9de9e68b94ae03a7` |
| N1 unary log | `87a62d47f74bdaa8d496e2291a8cf4b88d677103e25da217d2607728401e9402` |
| E2 unary report | `49a319362cdb377cb7ef400734071c9483b377c91f8bbd49a3e549ae6703fd25` |
| E2 unary log | `79adecc5b7c91366d7e7ad3edc08781b01cf873b05bcd2fb26eeb832d203c052` |

