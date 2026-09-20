# Rust 1.98.1 toolchain A/B on C3D, 2026-09-19

The pinned toolchain moved from Rust 1.97.0 to 1.98.1. This records what the
compiler change did to the measured paths, so the pin is kept on evidence rather
than on the assumption that a compiler upgrade is free.

## Design

One fixed source revision, `b8ee16934`, built twice. The only difference between
arms is which rustc produced `riffdbd` and the load runner. Comparing against the
banked 2026-09-19 figures would have confounded the compiler with the revisions
merged since that bank was taken, so both arms were re-measured in one session.

Arms ran counterbalanced — 1.97.0 then 1.98.1, then 1.98.1 then 1.97.0 — so any
monotonic host drift lands on both arms equally. Two repetitions per arm,
20-second measurement window after a 5-second warmup, RiffDB only, PostgreSQL
comparator skipped. Host C3D (`bench-host-c3d`, 8 vCPU), otherwise idle.

`benchmarks/run-app-baseline` selects its toolchain explicitly with
`cargo +1.97.0`, which overrides `RUSTUP_TOOLCHAIN`. A first run that set only
the environment variable therefore rebuilt the daemon with 1.97.0 in **both**
arms and would have reported a spurious null result. It was discarded before it
produced cells. For the recorded run the harness was patched on the bench host to
take the toolchain from `RIFFDB_BENCH_TOOLCHAIN`, identically in both arms, and
the patch was reverted afterwards.

`--smoke` reports carry no build provenance block, so the digests were taken
directly from each arm's target directory:

| toolchain | `riffdbd` sha256 |
|---|---|
| 1.97.0 | `27724df5e5b3797a2667e86b99e845921acff7f0fe2b67cd3282210fdb8d331c` |
| 1.98.1 | `84588be5cd8f528a7754d3888eaeabbec05f06bc4dfbec4d3c0e492a3f518175` |

They differ, so the arms measured different binaries. Equal digests would void
the comparison.

## Result: no measurable effect

Throughput in ops/s, mean of two repetitions, per-repetition values in brackets.

| load | clients | 1.97.0 | 1.98.1 | ratio | worst rep spread |
|---|---:|---:|---:|---:|---:|
| write_only | 1 | 668 [662, 674] | 680 [675, 684] | 1.017 | 1.018 |
| write_only | 8 | 2,649 [2654, 2644] | 2,642 [2650, 2635] | 0.998 | 1.006 |
| write_only | 32 | 3,563 [3556, 3570] | 3,524 [3515, 3533] | 0.989 | 1.005 |
| write_only | 128 | 3,740 [3736, 3744] | 3,781 [3738, 3824] | 1.011 | 1.023 |
| read_only | 1 | 3,858 [3878, 3838] | 3,860 [3872, 3849] | 1.001 | 1.010 |
| read_only | 8 | 19,268 [19291, 19244] | 19,324 [19313, 19336] | 1.003 | 1.002 |
| read_only | 32 | 29,379 [29419, 29339] | 29,477 [29482, 29472] | 1.003 | 1.003 |
| read_only | 128 | 31,908 [31855, 31962] | 31,750 [31790, 31710] | 0.995 | 1.003 |
| interactive | 8 | 10,916 [10905, 10927] | 10,952 [10939, 10966] | 1.003 | 1.002 |
| interactive | 32 | 17,538 [17455, 17621] | 17,576 [17597, 17554] | 1.002 | 1.010 |

Geometric mean ratio 1.98.1/1.97.0: write_only **1.0036**, read_only **1.0005**,
interactive **1.0027**.

Every cell's ratio falls between 0.989 and 1.017, and the worst within-toolchain
repetition spread is 1.023 — larger than the between-toolchain difference in
every cell but one. **The compiler change is not distinguishable from run-to-run
noise on this host.** Nothing here is evidence of a gain; the claim is only that
no regression was found at this resolution.

## What this does not establish

- Two repetitions per cell resolve effects of roughly 2% and larger. A
  sub-1% systematic change would not be detected.
- One host. C3D's AMD EPYC 9B14 exposes SHA-NI; the earlier statement that it
  lacked SHA-NI was incorrect. CPU and disk differences still prevent transferring
  this ratio to other hosts. See [measurement-host selection](benchmark-host-selection.md).
- The write_only c=1 cell (+1.7%) is the largest single ratio and also carries a
  1.018 repetition spread. It is noise, not a finding.
- Latency percentiles were recorded but are not compared here; p50 was identical
  across arms in every cell, and p95 moved only at write_only c=128, where the
  repetition spread is widest.

## Control check

The 1.97.0 arm reproduces the banked 2026-09-19 figures closely, which is the
evidence that the harness and host are behaving as they did for the bank:

| clients | banked `perf7-jemalloc` | this run, 1.97.0 |
|---:|---:|---:|
| 1 | 671 | 668 |
| 8 | 2,622 | 2,649 |
| 32 | 3,555 | 3,563 |
| 128 | 3,739 | 3,740 |

## Lint deferral carried by this upgrade

Rust 1.98 adds `chunks_exact_to_as_chunks`, which fires at 72 sites including
hot paths. It is set to `allow` at the workspace root with a comment recording
why: adopting it during the bump would have meant measuring a code change and a
compiler change together. Its adoption is a separate change with its own
measurement.
