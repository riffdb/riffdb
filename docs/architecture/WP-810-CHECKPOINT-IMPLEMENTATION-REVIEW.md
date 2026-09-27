# WP-810 checkpoint implementation and fixture review

**Draft: implementation and correctness evidence are ready; the shared-delay
disposition and human acceptance remain outstanding.** This document does not
request closure or replace the package's exit gate. The replacement diagnostic
now has complete backend-I/O and native-call coverage. Journal sync waits are
in file writeback and device flush; resolution of the shared tail is still open.

## Scope and source

Package: WP-810. Tier: guarantee. The completed diagnostics used source:
`31aeab76fa7c41b5d3a05d191905fd8b53e43dd2`, including main's index-warming
repair through `a1809310f`. The writeback prototype starts from `983881ea`,
which integrates main through `aa77f9622`; the older timing results do not
qualify that newer integration or the prototype.
The work package remains open. WP-749's archive-presence gate is separate.

The decisions are within ADR-0101, ADR-0104 and ADR-0132: keep bounded foreground
apply running during checkpoint materialization, then retain the ordered drain
required for checkpoint installation. No checkpoint threshold, journal/capsule
encoding, public interface, durability requirement or sequence owner changes.

## Implementation to review

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

### Bounded checkpoint-writeback prototype

The primary redb backend limits submitted checkpoint writes to 8 MiB between
successful syncs, adding an early sync before further writes would exceed the
budget. Oversized writes are split without copying their contents. This is a
fixed implementation experiment, not an operator setting or a millisecond
latency bound. Only the physical materialization transaction activates it;
ordinary writes and the derived sidecar do not activate the budget.

The checkpoint remains one redb transaction. Growth syncs, final Immediate
commit sync, exact installation, reclamation, command acknowledgement, and all
existing capacity limits stay intact. Early sync failure propagates through
redb and the existing failed-checkpoint fence. No early sync publishes roots.
A per-backend mutex serializes budget accounting with synchronous writes and
syncs; reads bypass it. The scope is acquired after the exclusive redb write
transaction and released after commit (or on error).

The same wrapper serves the real FileBackend and injected media. The new
proofs cover byte/offset preservation, scope isolation, final-fence retention,
early I/O failure, checked offsets, and a deterministic two-writer schedule.
A real redb transaction writes 24 MiB with the production 4 MiB cache and
proves complete commit or refusal on early-sync failure. Two abrupt child exits
before/after the first early sync prove that restart twice retains the old
complete transaction with no partially published rows. The existing six
production checkpoint/journal/publication crash edges remain mandatory.

The budget test failed against the pass-through backend before implementation.
All 14 targeted tests pass after implementation, including the new real-I/O
and process-crash tests. Broader acceptance and a same-baseline measurement
remain pending. No latency benefit or closure is claimed.

## Requirement and deliverable audit

| Requirement or deliverable | Evidence and present disposition |
| --- | --- |
| STO-001: semantic storage boundary | Production coordinator plus real-redb schedules above; first-party coordinator remains the only sequence/apply owner. |
| STO-002: explicit versioned encodings | Encodings are unchanged; generated-artifact checks and the full compatibility/recovery battery pass. The new schedule tests do not independently redefine encoding validity. |
| REC-001 / REC-002: idempotent recovery | The six crash edges reopen twice, compare complete authority and audit counts, and refuse invented successors. |
| PERF-018: attributable measurement evidence | The six-cell comparison retains exact sources, binaries, raw percentiles, host validity, checkpoint counts and signed wall remainders. It is a bounded comparison, not an alpha release sweep. |
| Standalone finding | [Performance report](../performance/wp-810-checkpoint-journal-tail.md), with retained archive-disabled trace and instrument limitations. |
| Current correctness qualification | `release/evidence/wp-810/index-warming-qualification-20260926.json`: full CI, 23 coordinator tests, 20 diagnostic tests and all 19,220 seed commands pass. Earlier failed invocations remain retained. |
| Reviewed shared-delay resolution | **Not met.** Native timing now locates measured slow journal sync time in file writeback (80.37%) and device flush (19.61%), with zero JBD2 commit-wait time and 0.02% residual. A measured mitigation and maintainer disposition remain outstanding. |
| Final human implementation/fixture review and closure | **Not met.** No acceptance or completion is recorded. |

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
This supplies the basis for one bounded-writeback prototype, not a closure.

## Remaining completion actions

1. Resolve the remaining concurrent checkpoint/journal sync delay and the
   package's exit gate with the maintainer. The bounded diagnostic is complete;
   a named operation alone does not satisfy the required reviewed resolution.
2. Obtain final implementation/fixture acceptance for the selected implementation.
3. Record acceptance, `completed_at` and decisions in the proposed closure;
   validate the final merge tree including that record, and land only after
   the required approval.

No additional campaign, format change or numeric threshold is authorized by
this checklist. The existing source-bound evidence must remain intact.
