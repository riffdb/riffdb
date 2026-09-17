# WP-787: removing fresh-locator coverage

ADR-0236 removed the ADR-0197 fresh-locator coverage mechanism. This note
records what was measured, what was inferred, and what remains unmeasured.

## Why the mechanism was removed

Coverage never armed in the running server. A counting build of the
`run-app-baseline` write-only smoke arm, eight clients over twenty seconds on
this workstation, reported the following over the load daemon.

| observation | count |
|---|---|
| arming decisions reached, any daemon | 0 |
| lookups finding coverage uninitialized | 1 |
| seals finding coverage disabled | 8788 of 8788 |
| seals finding coverage armed | 0 |
| operational absence resolutions | 81003 |
| answered by coverage | 0 |
| answered by the command-derived index | 80970 |
| answered by checkpoint equality | 32 |
| bounded history fallback scans | 0 |

Three facts explain it. ADR-0197 Decision 1 named the grouped admission entry
as the sole initialization site, and no production path calls that entry. The
live command pipeline writes through the ADR-0104 composite stage, which
carries no writer-private redb transaction, and Decision 1 excluded that stage
by name. The empty-authority proof also requires every authority table empty
and the transient index dormant, and a first start over a fresh database
rebuilds the index to ready while a start that leaves it dormant follows a
clean close and so has commits.

## The performance arms this replaces

Measured earlier on the GCP E2 bench host, write-only smoke, mean group-commit
time over two interleaved rounds. These arms were taken before removal and are
carried forward from ADR-0235.

| arm | c=1 | c=8 | c=32 |
|---|---|---|---|
| main | 1739 us | 2971 us | 6572 us |
| staged-locator repair | 1610 us | 2787 us | 6323 us |
| witness skipped | 1234 us | 1999 us | 4747 us |
| coverage disabled every seal | 1308 us | 2054 us | 4590 us |

The coverage-disabled arm is the ceiling this removal aims at: 25 to 31 percent
faster group commit with 16 to 21 percent more throughput.

## What the implemented removal measured

Interleaved A/B on the E2 bench host, write-only smoke, two rounds at each
client level, 20 second load with a 5 second warmup. The base is the merge base
`ffd761c5`, which already carries the staged-locator repair. Each figure is the
mean of the two rounds.

| clients | mean group commit | throughput | p50 |
|---|---|---|---|
| 1 | 2668 us to 2248 us, 15.8 percent faster | 149 to 158 ops/s, 6.0 percent more | 6.81 ms to 6.42 ms |
| 8 | 4186 us to 3420 us, 18.3 percent faster | 746 to 857 ops/s, 14.9 percent more | 10.23 ms to 9.18 ms |
| 32 | 10145 us to 8577 us, 15.5 percent faster | 1073 to 1184 ops/s, 10.3 percent more | 27.79 ms to 25.69 ms |

The realized gain is smaller than the coverage-disabled diagnostic arm
suggested, and the reason is the base. That arm was measured against `main`
before the staged-locator repair landed, so it included work the base here has
already taken. The two together account for the difference; neither figure is
revised.

## What this note does not claim

The bounded history fallback is now the only absence backstop. Its proof
asserts that the measured workloads never reach it and bounds what a scan may
cover. No workload in the suite exercises a scan, so the cost of the backstop
under a workload that does reach it is unmeasured.
