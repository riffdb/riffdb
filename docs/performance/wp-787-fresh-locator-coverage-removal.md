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

## What this note does not claim

The implemented removal has not been measured on the bench hosts. No speedup is
claimed here beyond the arms above, which measured a diagnostic build rather
than this change. The measurement to take is a fresh interleaved A/B of this
branch against its merge base on E2, at 1, 8 and 32 clients.

The bounded history fallback is now the only absence backstop. Its proof
asserts that the measured workloads never reach it and bounds what a scan may
cover. No workload in the suite exercises a scan, so the cost of the backstop
under a workload that does reach it is unmeasured.
