# WP-749: remaining archive-presence qualification

Status: the fixed archive comparison is complete; **WP-749 remains open on
measured acknowledgement overhead**. The
[correlated tail investigation](wp749-correlated-tail-2026-09.md) is closed.
WP-748 promotion is complete independently. Capture representation,
hashing and allocation experiments remain closed.

## Bounded comparison

Freeze the qualified integration candidate based on current public main,
including WP-748 promotion, the receipt-wait/checkpoint-counter repairs and
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

## Result recorded on 2026-09-21

The measured source is `11f1ffbc3f1ee1d579e971c3a0e694317ebc1a7d`, tree
`150c4c0d1e84c62bca15e5d053c47e1a6df9d921`. Both arms used identical daemon and
runner binaries. The repository evidence bank at
`release/evidence/wp-749/archive-presence-20260921/README.md`
contains their digests, every attempted cell, raw-report and exported-report
digests, the host observations, all five values per arm, paired differences and
a script that independently verifies the arithmetic.

| Workload | Metric | Archive off median | Archive on median | Change |
| --- | --- | ---: | ---: | ---: |
| Write-only | Throughput (ops/s) | 2,933 | 2,684 | -8.49% |
| Write-only | Request P50 (ms) | 9.437 | 9.961 | +5.56% |
| Write-only | Request P95 (ms) | 12.059 | 12.583 | +4.35% |
| Write-only | Request P99 (ms) | 96.469 | 121.635 | +26.09% |
| Write-only | Mean group commit (ms) | 3.219 | 3.482 | +8.19% |
| Interactive | Throughput (ops/s) | 15,304 | 14,599 | -4.61% |
| Interactive | Request P50 (ms) | 0.688 | 0.754 | +9.52% |
| Interactive | Request P95 (ms) | 9.961 | 9.961 | 0.00% |
| Interactive | Request P99 (ms) | 12.059 | 12.583 | +4.35% |
| Interactive | Mean group commit (ms) | 2.938 | 2.890 | -1.62% |

All twenty replacement cells passed host and correctness checks, with zero
whole-cell CPU steal, errors or unavailable outcomes. Every arm passed the
P50/P95 central-three stability rule. Every paired comparison had lower
archive-enabled throughput; every write-only pair also had higher P95. Stability
does not establish parity, and PERF-018's comparator thresholds are not an
archive-overhead allowance.

All ten enabled cells confirmed archive progress and retained a manifest digest,
with no observed collector terminal failure. Inferred terminal application lag
was zero in eight cells, 32 in one and 49 in one. The source frontier is inferred
from the backup frontier plus all process command commits, including warmup;
this is not an independent caught-up proof.

The first host attempt stopped at its fifth cell because postflight observed two
new SSH authentication/session processes. It reported neither an active
interfering process nor CPU steal, but the process-identity rule still failed.
Its five attempted reports, including four completed valid cells, are retained.
The SSH journal records a preauthentication close; the responsible actor is not
identified. ADR-0143 section 3's single host-invalid replacement allowance was
used to restart the complete twenty-cell set. No initial cell was imported, no
performance result selected a retry, and host policy was unchanged. The
replacement completed at 14:11:35 UTC; the host was released at 14:12:32 UTC.

P99 remains disclosure under ADR-0249. The earlier correlated investigation
named checkpoint-conditioned durability/publication drains; this comparison
does not attribute its additional archive tail movement to that mechanism and
does not open another tail hypothesis. The present closure issue is the measured
write-only acknowledgement cost. The existing gate's word "unchanged" is not
established by these results. Reducing that cost or explicitly accepting a gate
clarification remains separate work; no completion is recorded here.

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

The subsequent combined candidate `e9faa42c` joins WP-748 `db871a51` and
WP-749 `5c77333e` over main `05501ec07`. On 2026-09-21 it passed all 17 steps
of `./scripts/acceptance --range 05501ec07..HEAD --full` in one uninterrupted
invocation, including the complete `ci-all` battery. That run used another
fresh, dedicated build directory and the pinned language tooling. Both real
CLI archive cases passed, as did the source/sink crash, exact-stop, corruption,
compatibility and promotion proofs. See the
[combined verification record](../architecture/WP-748-LIFECYCLE-VERIFICATION.md#final-combined-integration-verification).
This settles final integration verification at that source identity; the
WP-748 implementation was subsequently approved and landed independently.
WP-749 changes remain on the review branch. The measured revision adds only
documentation to that qualified runtime; subsequent main integration changed
only catalog tests, documentation and package metadata. The added catalog test
passed separately. No archive-latency result is inferred from correctness tests.

## Readiness repair and PR note

Package WP-749; tier internal for this runner/documentation increment. Six
explicit selectors in `benchmarks/run-app-baseline` still requested Rust 1.97.0,
including its build receipt, while both workspaces require 1.98.1. Restore the
previously checked 1.98.1 correction so the standard wrapper and its receipt
agree with the actual build. Shell syntax, all six selectors against both
workspace minimums, and offline locked runner metadata resolution pass.
Public runtime behavior, formats and acknowledgement ordering are unchanged.
This linked handbook page records the protocol and integration result.
[Backup and restore](../backup-restore.md#restore-rewinds-history) now states
that archive restore rewinds to its selected restored frontier and supports
exact sequence recovery; it removes the stale blanket exclusion of incremental
backup while preserving the absence of a wall-clock recovery promise.
The measurement is now recorded above. This evidence increment is surface-tier under the `release/**` path rule,
following unchanged ADR-0143/0171/0249 decisions. It changes no runtime behavior
or durable bytes. It updates this page and the qualification
status in backup/restore; archive acknowledgement overhead, review and landing
remain required before completion.
