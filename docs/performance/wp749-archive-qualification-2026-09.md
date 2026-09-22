# WP-749: remaining archive-presence qualification

Status: **WP-749 is blocked on [WP-810](wp-810-checkpoint-journal-tail.md)**,
the shared checkpoint/journal package. The acknowledgement-latency gate is
unmet; the archive investigation is stopped. The fixed archive comparison is
complete, and the
[correlated tail investigation](wp749-correlated-tail-2026-09.md) is closed.
WP-748 promotion is already complete on main, dated 2026-09-21, and has no
dependency on this block. Capture representation,
hashing and allocation experiments remain closed.

## Blocked handoff and disclosure, 2026-09-21

**Acknowledgement P99 moves from 88.35 ms to 114.93 ms with archive enabled.**
The dominant observed mechanism is **checkpoint-coupled journal durability**,
present without archive and now owned by WP-810. A residual of roughly
**11.7 ms remains unattributed**. That value comes from the retained fixed-trace
sensitivity; it is not an independently measured archive-specific cost.
Attributing the remainder requires reducing the dominant shared term first.
Further subtraction of the existing samples cannot settle the closure decision.

The enabled **141.387 ms journal write inside a 142.265 ms receipt wait**
remains the concrete blocking observation; its sync takes only 0.799 ms.
All raw evidence and the limits of the earlier analyses remain retained in the
[isolated-pair report](wp749-archive-isolated-pair-2026-09.md) and
[decomposition record](wp749-archive-p99-decomposition-2026-09.md).

The maintainer directed this disposition in session on 2026-09-21: record the
named dominant mechanism and explicitly unattributed remainder as the
ADR-0249 disclosure, block WP-749 on its owner, and stop. Disclosure is not a
latency-gate pass. The accepted exit gate and REP-007 remain unchanged; no
numeric P99 gate or latency allowance is introduced.

The checkpoint package was initially numbered WP-806, then WP-808 and WP-809,
on this review branch. Main independently assigned those numbers to the semantic
write-path comparison, governance verification, and same-root revision fence
tasks, so the checkpoint package is renamed **WP-810**. The dependency follows
the same checkpoint/journal work.

### Main integration check, 2026-09-22

The merge of main at `57167a230ad7e46009f88965a7c2eafa44fc9dd6`
leaves this prerequisite unresolved. Since the branches' common ancestor,
main has changed no storage-redb implementation files; its commit-crate changes
admit the accepted V24 command plans and do not change checkpoint drains.
The checkpoint-conditioned drain and ordered-publication requirement remain
in the production path. No checkpoint mitigation or new reduction evidence
arrived with this merge. WP-749 remains blocked, and no measurement was run.

### Remeasurement that clears the block

1. WP-810 must land a guarantee-preserving reduction of the shared checkpoint
   term, supported by its own correctness and performance evidence. Closing the
   package without reducing that term is insufficient.
2. Then freeze refreshed source and the current pinned toolchain, record build
   and executable digests, and use identical binaries in one isolated archive
   disabled/enabled pair. Keep the full dataset, c32 write-only, Standard
   durability, 15-second warmup and 90-second measurement per cell. Coordinate
   fresh exclusive host use and apply the established reversible isolation.
3. Retain direct command-acknowledgement P50/P95/P99, host and correctness
   checks, repaired wait counters, checkpoint counts and archive progress.
   Disclose throughput separately. Compare the direct off/on measurements;
   do not subtract a modeled shared component. Preserve both cells and any
   failed attempt, with no automatic retry.
4. Assess the unchanged latency/write-gate clause from that evidence. If it is
   still unmet, an exact, separately accepted amendment would be required;
   this blocked state supplies no waiver or automatic completion.

No measurement is scheduled by this handoff. WP-749 remains blocked until the
prerequisite is met; its investigation does not continue in parallel with it.

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

Throughput is disclosed; this package's exit gate binds acknowledgement latency
and the write gate. ADR-0249 sets no numeric P99 threshold, but requires a named
mechanism for a published tail regression. The earlier checkpoint-conditioned
durability/publication drain does not automatically explain an additional
archive-presence effect. An unexplained movement cannot be discharged by naming
a follow-up owner. This protocol does not pre-award a pass.

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

For write-only, these request percentiles directly measure client-observed
command acknowledgement. `time_call` in `examples/app-baseline/core/src/timing.rs`
starts before the generated command call; the RiffDB backend returns after the
gRPC response is received and decoded. Thus P50, P95 and P99 all moved in this
comparison; there is no separate unchanged acknowledgement figure. Interactive
percentiles aggregate reads and writes and are not a write-only acknowledgement
measurement. Mean group-commit time is also a different population and boundary.

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

The existing gate's word "unchanged" is not established by these results. On
2026-09-21 the maintainer authorized one bounded archive off/on attribution,
using repaired receipt-wait and checkpoint counters joined to actual command
responses. It must verify the production construction and execution paths,
retain raw samples, and distinguish overlapping wait/sync intervals from the
unattributed client-duration remainder. It is not a new qualification campaign
or a reopening of capture representation work.

If that evidence establishes the mechanism and unchanged acknowledgement
latency, throughput cost is disclosed beside the gate verdict. If latency
genuinely moved, the required decision is an exact amendment to the latency
clause naming the mechanism, not an exception for throughput. Until then the
additional archive tail movement remains unresolved and WP-749 stays open.

The [bounded attribution attempt](wp749-archive-ack-attribution-2026-09.md)
subsequently completed both workloads but stopped at enabled-cell host postflight
after unauthenticated SSH activity. Its trace identifies long positional journal
writes before sync on the checkpoint-conditioned drain path; the host-invalid
pair does not settle the archive-specific cause. There was no retry, amendment
or completion claim.

The later [isolated replacement pair](wp749-archive-isolated-pair-2026-09.md)
passed all host checks. It confirms that checkpoint-coupled journal waiting
already exists with archive disabled and reproduces an additional long pre-sync
journal write in the enabled arm. The baseline checkpoint mechanism belongs to
storage/journal; its presence in both arms does not establish archive latency
parity or settle the resource-level cause of the incremental blocking. The
authorized bounded measurement is finished, with no further run or completion
claim. The twenty-cell results above remain the qualification evidence.

The [retained-request decomposition](wp749-archive-p99-decomposition-2026-09.md)
then calculated the isolated pair's 26.582760 ms P99 movement without another
run. Subtracting each request's own checkpoint pre-sync wait reduces that
contrast by 14.895930 ms and leaves 11.686830 ms. This is a fixed-trace
sensitivity, not a causal archive residual; drain-exposure standardization also
leaves a substantial within-state movement. The shared mechanism is now owned
by [WP-810](wp-810-checkpoint-journal-tail.md). A small named archive cost is
not established, so the unchanged-latency gate remains unmet and no amendment
or completion is justified by this arithmetic.

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
