# WP-749: remaining archive-presence qualification

Status: protocol prepared; **no new remote measurement yet**. The
[correlated tail investigation](wp749-correlated-tail-2026-09.md) is closed.
WP-748 promotion correctness proceeds independently. Capture representation,
hashing and allocation experiments remain closed.

## Bounded comparison

Freeze current public main plus the receipt-wait/checkpoint-counter repairs and
the standard benchmark runner's Rust 1.98.1 selector correction. Record the
source/tree, both Cargo locks, compiler, build flags and executed daemon/runner
digests. Build the daemon and runner in newly created, separate output directories
and bind those directories to the build receipt. The archive arms use the **same binaries**; only the existing
`RIFFDB_APP_BASELINE_ARCHIVE_COLLECTION=disabled|enabled` setting differs. Both
prepare the same stopped full backup outside the timed interval. The runner's
nested lock already resolves against current main; no new dependency is needed.

On one exclusively reserved 8-vCPU AMD EPYC 9B14 host, requiring SHA-NI, AES,
PCLMULQDQ and SSE4.2:

- Full frozen app-baseline dataset, 32 clients, Standard durability, 15 seconds
  warmup and 90 seconds measurement per cell, one repetition per invocation.
- Five pairs for write-only, then five for interactive: 20 cells total. Alternate
  disabled/enabled and enabled/disabled pair order. Each cell has a fresh database.
- Generic x86-64 release builds, Rust 1.98.1, thin LTO and one codegen unit for
  both daemon and runner. Hardware hashing is selected by the normal runtime.
- Production capture remains enabled. No private correlation patch or optional
  writer-stage observers participate in the latency comparison.
- Record each attempt before launch. Keep every report, P50/P95/P99, throughput,
  group-commit count/mean, host preflight/postflight and exact binary identity.
  Stop on failed correctness, invalid host, collector failure, timeout or an
  expired reservation. Do not retry or omit a measurement because of its result.
- Verify confirmed archive progress beyond the full-backup frontier and retain
  the terminal manifest digest. Report remaining archive lag with its method;
  do not infer caught-up status from successful shutdown.

## Interpretation

The archive-disabled cells refresh the current baseline. The historical
same-toolchain absolute controls and capture-cost evidence retain their original
source identities; they are not substituted for these controls or used to
attribute archive overhead. This comparison cannot discharge the full emitter,
PostgreSQL or multi-profile release campaign owned by WP-750.

Publish all five values per arm and their medians. ADR-0171's central-three
spread check (`x4 / x2 <= 1.20`) applies to P50/P95 evidence stability. **Within-arm
stability is not between-arm parity and does not grant 20% archive overhead.**
Disclose each paired difference and the median contrast against the same-source
disabled arm. The existing acknowledgement/write-gate requirement remains.

P99 is disclosed under ADR-0249, with the already named checkpoint-conditioned
durability/publication drain and the limits of that attribution. There is no
new numeric P99 threshold, no claim that every tail movement has that cause, and
no new tail hypothesis or representation work follows from this comparison.
Unresolved evidence remains unresolved; this protocol does not pre-award a pass.

## Local integration checks

On 2026-09-21, revision `65101f19` completed all 14 non-test package acceptance
checks, including all-target/all-feature Clippy, generated artifacts, handbook
and governance, in an isolated build directory. Its selected test run passed
1,543 tests and failed one nested release-manifest check; nine existing opt-in
tests were skipped. The failed test then passed in the isolated directory.
All 1,544 selected tests therefore have passing results across the retained
attempts; the initial invocation remains recorded as failed.

The initial Clippy, generator, handbook and nested manifest builds had reused
artifacts containing WP-748-only enum variants absent from this source checkout.
Fresh build isolation removed those errors without a source change. The Python
environment also needed its existing PyYAML dependency alongside the pinned
Maturin builder. These environment repairs are local verification findings.
The planned measurement builds now require fresh output directories as described
above. This is package readiness evidence; archive qualification and final
integration checks remain required before closure.

## Readiness repair and PR note

Package WP-749; tier internal for this runner/documentation increment. Six
explicit selectors in `benchmarks/run-app-baseline` still requested Rust 1.97.0,
including its build receipt, while both workspaces require 1.98.1. Restore the
previously checked 1.98.1 correction so the standard wrapper and its receipt
agree with the actual build. Shell syntax, all six selectors against both
workspace minimums, and offline locked runner metadata resolution pass.
Public runtime behavior, formats and acknowledgement ordering are unchanged.
This linked handbook page records the protocol; measurement and final merge
checks remain required before qualification or completion.
