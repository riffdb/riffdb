# WP-810 checkpoint implementation and fixture review

**Accepted by the maintainer on 2026-09-27.** In response to the final review
at `0a71cadf`, the maintainer said **"i accept it"**. This accepts the
implementation and fixtures at runtime source `f6b4315b` and the limited
mitigation, including the remaining shared journal delay, as WP-810's reviewed
resolution. Integration `ccadaf50`, including main `dfd16a1e`, passes all 16
acceptance steps including the complete CI battery.
WP-749's prerequisite and archive-presence gate remain separate.

## Accepted disposition

The bounded mitigations below constitute the maintainer-reviewed resolution,
with the remaining journal tail explicitly accepted. No additional benchmark
or deeper checkpoint-I/O redesign is required to close WP-810. This disposition
accepts the implementation and the disclosed limitation together; it changes no
durability guarantee or performance threshold.

The evidence supports retaining the pipeline correction. Its earlier unchanged-
control comparison improves throughput/P95 and removes the checkpoint-wide
writer drain while preserving installation safety. The later identical-write
candidate is a modest further mitigation: P99 improves substantially in two
pairs and is nearly flat in one, with no throughput improvement.
**It does not demonstrate removal of the shared
tail**, and no predictable P99 reduction or sufficient reduction for WP-749 is
claimed. The maintainer's acceptance explicitly retains that limitation.

## Scope and source

Package: WP-810. Tier: guarantee. The completed native diagnostic used source:
`31aeab76fa7c41b5d3a05d191905fd8b53e43dd2`, including main's index-warming
repair through `a1809310f`. The later writeback comparison uses control
`983881ea`, including main through `aa77f9622`, and prototype `d4735146`.
`0e85750c` withdraws that prototype and restores the control runtime exactly.
The latest fixed comparison uses control `df7ad5b2` and candidate `f6b4315b`,
including the same main `aa77f9622`; the control runtime equals the restored
`983881ea` runtime. Full CI passes on `f6b4315b`. The final integration
`ccadaf50` additionally includes main's retention repair at `dfd16a1e` and
passes full acceptance/CI. Measurements remain bound to their original
sources. WP-749's archive-presence gate is separate.

The decisions are within ADR-0101, ADR-0104 and ADR-0132: keep bounded foreground
apply running during checkpoint materialization, then retain the ordered drain
required for checkpoint installation. No checkpoint threshold, journal/capsule
encoding, public interface, durability requirement or sequence owner changes.

## Accepted implementation

In `crates/riffdb-storage-redb/src/store.rs`:

- Command sealing uses `async_checkpoint_installation_pending`, rather than
  materializer registration alone, to request the pipeline drain. Completion
  racing a seal is observed by the next epoch-opening poll.
- `poll_async_checkpoint_locked` refuses an observed materialization failure
  before admitting more apply, even with an outstanding publication prefix.
  Successful installation still requires quiescence and exact checked roots.
- `ensure_journal_frame_headroom` publishes only already-submitted FIFO
  predecessors under capacity pressure. The staged next frame is excluded;
  original completion tickets remain valid. The retry bound is five steps.
- Rebase precedes extent reclamation. Existing unpublished, retained suffix,
  encoded-byte, physical-extent and overlay-memory bounds remain enforced.

The compiled limits remain 256 unpublished transitions / 16 MiB, an 8,192
transition / 32 MiB retained recovery suffix, a 40 MiB data region per physical
extent, and a 128 MiB composite overlay. The 4,096-transition / 16 MiB
checkpoint triggers and existing physical-reserve trigger are unchanged.

The admission scan-reuse prototype and checkpoint-I/O observation patch are
retained as evidence outside production. The first post-restart command-index
rebuild restored by main remains a disclosed baseline cost.

## Fixtures and semantic assertions

`tests/storage_recovery/checkpoint_pipeline.rs` uses real I/O and explicit
bounded hooks, with no sleeps or fabricated receipts:

| Proof | What is asserted |
| --- | --- |
| `checkpoint_materialization_allows_pipeline_but_installation_requires_publication` | Materialization and real journal sync pause independently; durability alone exposes no result or state; publication precedes installation and reclamation. |
| `unpublished_capacity_wait_publishes_the_prefix_without_losing_original_tickets` | The real 256-transition ceiling blocks the 257th until predecessor publication; every original completion ticket remains exact. |
| `failed_checkpoint_refuses_new_apply_while_a_durable_predecessor_awaits_publication` | Observed failure fences new apply; the already durable predecessor still publishes and survives restart. |
| `checkpoint_pipeline_process_crashes_recover_exactly_and_idempotently` | Six abrupt process exits cover before sync, durable/unpublished, checkpoint staged, checkpoint durable, installed/before reclaim, and reclaimed. Two restarts retain exact atomic authority. |

Immediate reads compare entity, outcome, commit, provenance and event values;
restart also checks audit counts and absence of invented successors. Before
sync, an unacknowledged complete frame may survive or disappear, but the result
must be atomic and stable across the second restart.

The coordinator schedule in `tests/command_semantics/checkpoint_pipeline.rs`,
`writer_pipelines_during_checkpoint_without_releasing_results_or_effects_early`,
proves that later apply/seal can proceed while the predecessor's real journal
sync is paused. Responses and application notifications remain withheld until
ordered publication. Two older grouped-command tests now wait for the existing
first-admission notification; their outcome and durability assertions are intact.
No durable-format or protocol golden fixtures change.

### Withdrawn early-sync experiment

The [retained experiment](../performance/wp-810-checkpoint-journal-tail.md#rejected-checkpoint-early-sync-prototype)
added an 8 MiB checkpoint backend-write budget with earlier syncs and preserved
all existing fences and limits. It passed 1,045 storage tests, 294 coordinator
tests, scoped acceptance, full CI and exact-binary full-workload qualification.
Its new proofs covered real engine sync failure, deterministic budget ownership,
and abrupt crashes before/after an early sync with repeated reopen.

Three fixed counterbalanced pairs all regress: median paired throughput
−67.75%, P50 +19.42%, P95 +1,104.08%, P99 +34.57%. All host and correctness
checks pass; raw-data reproduction is byte-identical. Added checkpoint syncs
raise mean journal sync from about 1.6 ms to about 10.3 ms, despite lowering
its maximum. This implementation is rejected and removed, with its patch and
exact binaries retained. No budget tuning, format change, merge or completion
follows. The selected pipeline implementation described above is unchanged.
The restored control passes 16 targeted checkpoint, retention and receipt
tests; the full-CI result above belongs to the withdrawn prototype.

### Measured byte-identical-write candidate

`f6b4315b` is a separate implementation experiment. In the production
asynchronous checkpoint's finalization scope, its backend compares page
writes byte-for-byte against the current file and omits complete equality.
Every growth and final sync remains unconditional. The fixed scratch buffer
is 4 KiB; the engine cache stays 4 MiB. All backend writes, resizes and syncs
share the comparison mutex, so reader-driven eviction cannot invalidate an
equality check before omission. I/O failures still fail the commit and fence
new writes. The journal path and mutation replay order are unchanged.

The [count reproduction](../performance/wp-810-checkpoint-journal-tail.md#byte-identical-checkpoint-finalization-writes)
proves redundant writes in pinned redb, without assigning their production
latency cost. The actual server storage opener installs this wrapper and the
production asynchronous checkpoint worker enters the finalization scope.
Nine targeted tests, 1,046 storage tests, 294 coordinator tests, six scoped
acceptance steps and full CI pass. New fixtures cover complete equality,
changed trailing bytes, extension/overflow, deterministic competing backend
operations, propagated failures, real-engine final-sync failure and two
process-crash edges with repeated exact reopen. No protocol or durable-format
golden fixture changes.

Both exact remote-built servers pass full 19,220-command seed and complete
c32 local workload qualification before the comparison. All six fixed remote
cells pass host and correctness checks. Distinct binaries, raw arrays and all
attempts are retained; raw recomputation gives byte-identical results.

| Median paired change | Result |
| --- | ---: |
| Throughput | +0.06% |
| P50 | +0.48% |
| P95 | +1.29% |
| P99 | −9.30% |
| Mean journal sync per flush | −2.78% |

The three P99 changes are −10.61%, −0.40% and −9.30%. Mean sync improves in
two pairs and worsens 1.94% in one. Candidate sync maxima remain 154.0, 210.0
and 169.1 ms. ADR-0239's 2–4% throughput variance supplies no P95/P99 noise
band, and P99 repeatability is unqualified. The paired observer does not join
native I/O to individual requests or count production bytes omitted. Extra
reads and serialization are real implementation costs; cold-cache and other
workload effects are not qualified by this c32 write-only comparison.

The [complete result](../performance/wp-810-checkpoint-journal-tail.md#byte-identical-checkpoint-finalization-writes)
includes all cell values, signed wall remainders, input digests, limitations,
checks and cleanup receipts. The maintainer accepted the limited measured
effect on 2026-09-27; the rejected early-sync strategy remains withdrawn.

## Requirement and deliverable audit

| Requirement or deliverable | Evidence and present disposition |
| --- | --- |
| STO-001: semantic storage boundary | Production coordinator plus real-redb schedules above; first-party coordinator remains the only sequence/apply owner. |
| STO-002: explicit versioned encodings | Encodings are unchanged; generated-artifact checks and the full compatibility/recovery battery pass. The new schedule tests do not independently redefine encoding validity. |
| REC-001 / REC-002: idempotent recovery | The six crash edges reopen twice, compare complete authority and audit counts, and refuse invented successors. |
| PERF-018: attributable measurement evidence | The six-cell comparison retains exact sources, binaries, raw percentiles, host validity, checkpoint counts and signed wall remainders. It is a bounded comparison, not an alpha release sweep. |
| Standalone finding | [Performance report](../performance/wp-810-checkpoint-journal-tail.md), with retained archive-disabled trace and instrument limitations. |
| Current correctness qualification | `release/evidence/wp-810/closure-20260927.json`: all 16 final acceptance steps, including complete CI, pass on integration `ccadaf50`. The earlier packet binds the original 1,046 storage tests, 294 coordinator tests, nine targeted proofs and exact-binary qualification to `f6b4315b`. Earlier failed/superseded attempts remain retained. |
| Reviewed shared-delay resolution | **Accepted, 2026-09-27.** Native timing locates measured slow journal sync time in file writeback (80.37%) and device flush (19.61%), with zero JBD2 commit-wait time and 0.02% residual. The maintainer accepts the limited mitigation and remaining tail together. |
| Final human implementation/fixture review and closure | **Accepted, 2026-09-27:** "i accept it", referring to review `0a71cadf` and runtime `f6b4315b`. The manifest records `completed_at`; the complete final integration battery passes at `ccadaf50`. |

## Performance evidence and its limits

The [three paired comparisons](../performance/wp-810-checkpoint-journal-tail.md#bounded-comparison-result)
compare control `d9ae2633` with candidate `66c394fc` on the same baseline,
toolchain and observation patch. Median paired changes are +5.06% throughput,
−2.80% P50 and −12.62% P95. P99 changes are +23.40%, +9.92% and −12.33%.
Those results belong to those exact sources, not the later integration.

The writer drain shrinks, while queueing behind long journal service remains.
Aggregate counters do not assign the P99 changes to individual requests or
establish a kernel/device cause. ADR-0249 does not supply a numeric P99 gate;
it still requires a named mechanism for a published regression.

The frozen diagnostic at
`release/evidence/wp-810/materialization-probe-20260926/index-warming-measurement-protocol.json`
allows one archive-disabled c32 cell on admitted exclusive hardware, with a
900-second total deadline, 15-second warmup and 90-second measurement. It
requires rebuilt observer binaries and exact-binary local qualification. It
measures overlap of checkpoint phases and actual backend I/O, preserving
unexplained time; overlap alone is not device causation. A result informs the
disposition and does not automatically close WP-810 or WP-749. Its one remote
cell completed, but 5,248,596 I/O records were dropped beyond the fixed
4,194,304 limit. The analyzer refused complete attribution. The earlier local
long-case overflow should have blocked remote execution; the subsequent short
functional pass did not prove full-cell capacity. Both local cases and the
remote failure are retained in `release/evidence/wp-810/materialization-overflow-20260926.json`.

The replacement probe is locally qualified for the full workload, retaining
12,924,427 I/O records and 139 checkpoints without drops. Its larger fixed
capacity and stricter full-workload gate are recorded in
`release/evidence/wp-810/materialization-capacity-20260926/qualification.json`.
The exact rebuilt remote binaries subsequently pass the same full local
qualification with 11,801,889 I/O records and zero drops. The authorized one-cell
replacement then passes both host checks, full trace validation, exact joins
and wall reconciliation: 282,003 measured requests, 105 byte-triggered
checkpoints and 9,431,998 I/O rows, without drops or failures. The numerical
result, binary identities, witnesses and retained-evidence digests are in
`release/evidence/wp-810/materialization-capacity-20260926/result.json` and
`derived-result.json`.

All 43 measured journal syncs over 100 ms occur entirely during checkpoint
durable commit and entirely inside an actual checkpoint backend sync call.
The clearest witness is 156.209 ms of journal sync inside a 156.302 ms receipt
wait, with checkpoint 39 concurrently syncing for 192.498 ms. No-checkpoint
journal syncs peak at 3.056 ms. The maximum measured journal write is 0.741 ms.
This names the blocked operation and overlap, not the kernel/device cause or
a demonstrated tail reduction. Raw P50/P95/P99 are 8.022/10.741/97.471 ms;
single-cell observer timings do not replace the earlier paired comparison.
The [performance report](../performance/wp-810-checkpoint-journal-tail.md#complete-replacement-journal-sync-overlaps-checkpoint-commit-sync)
records the full limits and signed wall remainders. C3D is released and the
task-owned remote build/data directories are removed.

The subsequent kernel diagnostic in `release/evidence/wp-810/kernel-io-20260927/`
joins all 21,074 native journal calls to their application flushes and selects
18,029 measured flushes. Its longest witness is 161.775 ms of native sync
inside a 161.970 ms receipt wait. File writeback and device flush account for
99.98% of the native time in the measured syncs above 10 ms; ext4 journal
transaction-commit wait accounts for zero. All application/native checks pass.
An earlier bpftrace allocation failure is retained separately; optional stack
formatting was removed and the collector stress-qualified before replacement.
This supplied the basis for the rejected early-sync prototype and the later
identical-write candidate. Neither diagnostic automatically closes the package.

All 125 files from the later comparison are verified locally, and its analyses
reproduce from the raw data. Exact source, binaries and original private logs
are retained. Host isolation is restored, no task processes remain, and 2.91 GB
of task-owned remote files are removed. C3D is idle; the standing exclusive-use
grant remains unchanged. Only main and this unfinished implementation worktree
remain registered locally.

## Closure and integration

The manifest records the accepted disposition, `completed_at`, implementation
decisions and remaining hazards. Original evidence packets retain their
prospective and measurement-time statuses; they are not rewritten to imply
earlier acceptance. `release/evidence/wp-810/closure-20260927.json` binds the
successful 16-step final acceptance, complete CI, source tree, log digest and
key production proofs to `ccadaf50`. It also retains the earlier CI attempt
stopped when main advanced, and explains the handbook/template warnings.
Only documentation and this validation receipt follow the checked integration;
their scoped acceptance runs before landing. The finished worktree is removed
after main retains the complete implementation and evidence.

The maintainer's in-session acceptance satisfies AGENTS.md's implementation/
fixture review and WP-810's reviewed-resolution requirement. No additional
approval, format or numeric threshold is proposed. The source-bound evidence
and WP-749's separate gate remain intact.
