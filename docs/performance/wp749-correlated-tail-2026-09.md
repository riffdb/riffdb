# WP-749: correlated checkpoint and journal-wait result

Status: **tail investigation closed after its one authorized run**. Measurement:
2026-09-21 UTC; maintainer direction: 2026-09-20. WP-748 is unpaused. No further
hypothesis or experiment belongs to this investigation. Capture representation
work and lifecycle consolidation remain closed and on hold, respectively.

## Finding

**While a checkpoint is registered, the existing pipeline drain makes the writer
wait for journal durability and publication. Long journal syncs appear inside
the first receipt waits of the same groups that contain tail requests.** This
is the named blocking path supported by the trace and the existing source.

Of 3,132 requests at or above raw P99, **2,896 (92.46%)** belonged to groups whose
actual drain predicate was true. Those groups account for 43.76% of all measured
requests. Tail requests' mean first wait was **55.983 ms**, and their mean journal
sync was **55.886 ms**, entirely inside that wait. These are overlapping
request-weighted intervals, not additive writer stages. Storage publication
averaged 0.614 ms for the same tail requests.

A concrete join: request sequence **340,022** took **148.129 ms**. Its group
21,445 recorded checkpoint 107 as active, a **139.094 ms** first wait, a
**139.019 ms** sync wholly inside that wait, and **0.283 ms** publication. The
same-request join and checkpoint lifecycle are retained in
[joined examples](evidence/wp749-final-correlation-20260921-joined-examples.json).

| One sample per measured group | Drain false | Drain true |
| --- | ---: | ---: |
| Groups | 11,194 | 8,568 |
| First-wait P99 | 1.037 ms | 72.534 ms |
| Journal-sync P99 | 0.903 ms | 72.424 ms |
| Maximum journal sync | 3.685 ms | 139.019 ms |

The source join is `store.rs::async_checkpoint_in_flight` (the registered
checkpoint remains present until installation), the unchanged command-fence
predicate, and `audit_executor.rs`'s `POST_DRAIN_ALL` branch. That branch drains
completion results after submission; it preserves the checkpoint-installation
ordering guarantee. **The drain remains intact.** The trace does not isolate the
operating-system reason each sync is slow or establish a counterfactual where
the drain is removed.

## What remains outside the measured group interval

The entire tail is not assigned to journal waiting. Tail requests averaged
**123.196 ms** end to end; their own groups spent **56.643 ms** from submission
through storage publication. The remaining **66.553 ms** is retained explicitly.
This subtraction compares nested durations; it does not compare the two
processes' clock origins or identify a queueing stage.

The slowest 18 requests, **158.906–160.262 ms**, belong to group 19,623 after its
checkpoint was installed. Their own sync took 0.696 ms and submission through
publication took 6.070 ms. Immediately preceding group 19,622 had a **133.129 ms**
sync while checkpoint 98 was registered. The next two slowest requests follow
the same pattern: their own group 12,169 has a 0.631 ms sync, immediately after
group 12,168's **124.718 ms** sync under checkpoint 61. Carry-over queueing is
consistent with those adjacent groups, but request admission is not timestamped
by this probe and that explanation remains an inference. All 20 raw durations
and their actual group joins are retained; none is relabeled as a long own-group
journal wait.

This names the observed wait mechanism without claiming every nanosecond, the
full historical parent/child P99 difference, or a checkpoint-off comparison.
Per maintainer direction, the investigation ends here. There is no numeric P99
pass/fail claim and no new hypothesis opened to account for the remainder.

## Run and coverage

| Measurement | Result |
| --- | ---: |
| Successful measured requests / errors / unavailable | 313,174 / 0 / 0 |
| Measured throughput | 3,479 operations/s |
| Raw P50 / P95 / P99 | 7.392 / 10.446 / 76.833 ms |
| Measured groups | 19,762 |
| All load-process groups, including warmup | 23,251 |
| Checkpoints selected / finally materialized / finally installed | 116 / 116 / 116 |
| Byte-triggered / transition-triggered | 116 / 0 |
| Selected transitions, minimum–maximum | 2,991–3,307 |
| Selected bytes, minimum–maximum | 16,795,353–18,176,216 |
| Missing, duplicate, unjoined or dropped evidence | 0 |

The ordinary checkpoint census is sampled before graceful close: it reports
116 materializations and 115 installations. The post-close event drain retains
the final installation. Both describe the same 116 selections, and all selection
sums, minima, maxima and crossed-limit counts reconcile exactly. Every measured
request has a unique commit-sequence/group join. Group totals also reconcile to
writer command and physical-commit counts. The trace also reproduces the repaired census's **46,502 wait calls** and
**139,094 us maximum wait**, plus all **23,251 flushes**, their **28,798,717 us**
summed sync and **139,019 us** maximum sync. The difference between the census's
all-wait sum and the trace's first-wait sum is only **34 us** of ready rechecks
at the census's microsecond resolution. These are consistency checks of the same
instrumented boundaries, not an independent observer-overhead measurement.
Host preflight and postflight passed, as did workload correctness. There was one attempt and no retry.

`POST_DRAIN_ALL` is **22.58% of accounted iteration time** in the existing writer
census. That denominator is not independently measured continuous writer wall.
The [W1/W2 warnings](wp749-writer-census-defects.md) still apply; busy plus idle,
nested stages and overlapping journal intervals are not silently summed into a
new wall-clock attribution.

The source was frozen at `1ba5be310b8f7f973d77885b14c19207aacde2be`, based on
main `68f696b5006295b5c8ec285db93564a108534fe6`, plus the privately retained
correlation patch. Main continued moving afterward. This is evidence about that
frozen source, not a claim to have qualified the newest main. The public summary
is the [numerical summary](evidence/wp749-final-correlation-20260921-summary.json); it contains
raw percentiles, slowest samples, group distributions and artifact digests.
Raw logs, complete arrays, reconstruction inputs and both executed binaries are
banked privately with 31 verified second-copy file digests. The temporary remote
source and database were removed; the host was released at 01:38:58 UTC.

## Final correlated diagnostic protocol

The maintainer authorized one final correlated run on the 8-vCPU AMD EPYC 9B14
host. It is the endpoint of the tail investigation, not a new representation
experiment. ADR-0249 makes P99 disclosure with a named mechanism; it supplies no
numeric P99 gate. Existing throughput, P50/P95 and archive-presence requirements
keep their meanings. The production-capture sync cost has a separate
[tracked issue](https://github.com/riffdb/riffdb/issues/2).

The following switches apply to the privately banked experimental patch;
ordinary repository builds do not include this one-run correlation probe.
With both `RIFFDB_WRITER_CORRELATION=1` and
`RIFFDB_WRITER_BATCH_DIAGNOSTICS=1`, the private diagnostic retains at most 65,536
command-group rows and 512 checkpoint rows per process. The supervisor emits
completed rows in pages of at most 1,024, outside the writer. It emits them to
stdout, keeping the harness's bounded stderr ring available for startup and
failure diagnostics. This is an opt-in investigation aid, not a durable format,
a public application interface, or a production performance setting.

A group row binds its first and last application sequence to journal submission,
physical flush identity and sync interval, first receipt-method wait, storage
publication interval, the existing pipeline-drain predicate and active checkpoint
identity. A checkpoint row retains selection size, crossed limits and
start/materialized/installed times. Each process has one monotonic time origin;
client and server clock values are never compared. The client retains bounded
raw request durations paired with the commit sequence returned by the existing
command outcome. The join uses that sequence, including every measured request.

The observer records first-wait entry even when the receipt is already ready;
that is not necessarily blocked time. Sync intervals shared by multiple groups
must be deduplicated by flush identity. Wait, sync and publication intervals can
overlap and must not be summed into writer wall. The drain flag records the
actual existing predicate, not a new checkpoint policy. The source still waits
for completion/publication while that predicate requires a drain.

Collection fails validation if request identities are missing or duplicated,
any measured sequence has no unique group, a trace buffer overflows, or final
emitted counts differ from reserved counts. Interrupted collection remains
partial evidence. The process-wide diagnostic assumes the daemon's one database;
it is not a multi-store tracing service. Observer overhead has no independent
off/on verdict, so this single instrumented run cannot quantify a production
regression or qualify archive-presence parity.

The local debug collection verified all measured joins, 688 completed group
rows and three installed checkpoints with no dropped evidence. Its timings are
inadmissible for performance attribution. The analyzer's negative checks refuse
missing/duplicate identities, dropped rows and incomplete final coverage. The
remote protocol uses Rust 1.98.1, release with thin LTO and one codegen unit,
smoke write-only at 32 clients, 15 seconds warmup and 90 seconds measurement,
one repetition. It retains every attempt and permits no automatic retry.

## Closure boundary and separate follow-up

ADR-0249 governs P99 disclosure. This run supplies a named mechanism and explicit
limits; the old numeric P99 hold is removed. WP-748 promotion work resumes under
its accepted records, with no lifecycle consolidation and no source-owned
projection-generation assertion reinstated after ADR-0248.

**Formal WP-749 completion still has a separate unfinished requirement:** its
manifest requires fresh archive-presence acknowledgement/performance evidence.
The prior refreshed campaign stopped before the archive off/on cells, and this
run has the archive consumer disabled. It cannot be substituted for that
comparison. Final package/merge checks also remain. Keep `closure.status: open`
until those requirements are met or their accepted scope is explicitly changed;
they are not another tail hypothesis and do not keep WP-748 paused.

The historical approximately 25% production-capture journal-sync cost is owned
as a separate storage/commit performance follow-up in
[issue #2](https://github.com/riffdb/riffdb/issues/2). Its historical measurement is
not a present-host qualification or an explanation of the complete P99 delta.
Filing it schedules no additional capture representation work.

## Review note

- Package: WP-749 investigation; tail investigation complete, formal package open.
- Tier: internal result and private diagnostic within accepted semantics.
- Behavior: retain exact sequence joins and disclose the observed checkpoint/
  journal-wait mechanism with its unassigned remainder; lift the WP-748 pause.
- Checks: 1,543 scoped tests passed with the private probe (nine existing profile
  skips), all 43 benchmark-core tests, four analyzer refusal tests, the local
  end-to-end join check, server all-target/all-feature build, scoped Clippy and
  one host-valid release run. The client-only dependency build reports five
  existing unused server-codec warnings; the full-feature scoped Clippy is clean.
  Full merge CI is pending.
- Handbook: this page, the receipt-wait repair follow-up and capture closure status.
- Compatibility: no durable format, checkpoint ordering or application API change.
- Hazards: observer overhead unmeasured, request admission uninstrumented, no
  historical-delta counterfactual, W1/W2 remain, archive-presence gate unqualified.
