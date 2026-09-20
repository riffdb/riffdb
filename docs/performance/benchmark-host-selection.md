# Choosing performance measurement hosts

Hardware SHA-2 acceleration is required for future RiffDB measurements by the
maintainer's 2026-09-19 direction. Stop testing on N1; its existing measurements
remain historical evidence. The profile retirement and enforcement changes are
being recorded separately. This guidance does not claim that the current daemon
enforces a CPU requirement at startup or amend an existing release gate.

## Accelerator support can change the conclusion

The [WP-749 capture investigation](wp749-capture-attribution-2026-09.md) ran
identical Rust 1.98.1 daemon and runner bytes on two 8-vCPU hosts. They used normal
`sha2` runtime dispatch, without a forced software backend.

| Historical host label | CPU | SHA-NI exposed | Capture hashing per group | All capture / writer wall |
|---|---|---|---:|---:|
| N1 (retired from testing) | Intel Xeon, family 6/model 63 | No | 491.040 us | 5.65% |
| C3D | AMD EPYC 9B14 | Yes | 63.294 us | 3.75% |

Hashing took **7.8 times longer on N1**. On C3D, eliminating all measured
capture hashing could save at most **1.41% of writer wall**, before accounting
for replacement work or overlap. An optimization priority inferred on N1 can
therefore reverse on a deployment CPU with hardware SHA-2. This is a first-class
host-selection result, independent of the closed capture optimization.

The observed ratio is a cross-host comparison, not an isolated instruction-set
experiment: CPU architecture, clock, OS and storage can also differ. Do not
normalize host results by 7.8, pool their denominators, or transfer a bottleneck
ranking between them. Keep hardware-specific findings beside each result.

## Before choosing a host or a baseline

- Verify hardware SHA-2 support exposed to the measured process: SHA-NI on
  x86-64 or the corresponding ARMv8 crypto support. Record CPU specifications,
  architecture and relevant feature flags, without real machine addresses.
- Verify the actual hashing dependency and backend dispatch. The investigation
  used `sha2 0.11.0` and `cpufeatures 0.3.0`; its x86 dispatch required SHA,
  SSE2, SSSE3 and SSE4.1. Record compiler flags and any backend override. Hardware
  capability alone does not prove that a binary uses it.
- Freeze source, lockfile, toolchain, allocator, release profile, binary digests,
  workload and storage placement. Refresh both controls after a toolchain or
  performance-programme change. Rust 1.97 timings cannot select a Rust 1.98.1
  candidate. Name any deliberate historical-profile differences.
- Coordinate an exclusive host interval with everyone using it. A lock private
  to one runner cannot exclude unrelated builds or benchmarks. Preserve every
  host-validity refusal and stop when a cell is ineligible.
- Attribute cost against independent writer wall using the
  [writer-census reconciliation](wp749-writer-census-defects.md). `busy + idle`
  is incomplete; nested stages and journal-thread work cannot simply be added.
- Predeclare mean and **P99** selection gates. A byte reduction, correctness
  pass or unmeasured candidate is not evidence of acceptable tail latency.

These rules complement [benchmark integrity](benchmark-integrity.md). Existing
release-profile requirements and their accepted successors govern qualification;
a host substitution or historical diagnostic does not itself pass those gates.
