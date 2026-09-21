# WP-749: bounded archive acknowledgement attribution

**The two-cell run stopped at enabled-cell host postflight. It is not a
qualified comparison and does not close WP-749.** Both workloads completed and
their complete traces reconcile. No retry or additional hypothesis run occurred.
The host was released at 17:55:09 UTC on 2026-09-21 after banking evidence and
removing the temporary source, database and build directories.

## Observed blocking path

The retained enabled trace contains long **positional journal writes before
sync**, in addition to the previously observed checkpoint-conditioned sync waits.
Its positional-write counter reached **169.888 ms**. Exactly one group, 17,665,
has a submission-to-sync interval long enough to contain that maximum:
**169.980 ms**. Each flush carries one command frame; every other enclosing
interval is shorter than the maximum write. This is a unique assignment by
interval containment, not a new per-write probe.

Group 17,665 covers sequences 299,343–299,358 while checkpoint 88 is registered
and the actual drain predicate is true. It records **197.502 ms** in its first
receipt wait, including **27.542 ms** in sync; its sixteen measured requests
take **208.811–210.014 ms**. The production path is the journal worker's
`write_all_at` interval, followed by `sync_data`, receipt completion and storage
publication. `POST_DRAIN_ALL` waits for that completion while the checkpoint
remains registered. The drain and its installation guarantee are unchanged.

Thus “all journal waiting is slow sync” would be wrong for these observations.
The trace identifies pre-sync journal-write blocking. It does **not** identify
the kernel/device cause, prove that archive I/O caused it, or establish a valid
archive-presence performance contrast on this failed host interval.

## Retained numbers and their limits

These are diagnostic observations, **not a qualified off/on effect estimate**.
Do not use the invalid enabled cell to replace the earlier five-pair results.

| Observation | Archive disabled | Archive enabled, host-invalid |
| --- | ---: | ---: |
| Successful measured commands | 263,523 | 238,823 |
| Raw acknowledgement P50 / P95 / P99 (ms) | 9.505 / 11.756 / 90.501 | 9.915 / 12.633 / 124.269 |
| Actual checkpoints, including warmup | 98 | 89 |
| P99-tail requests with drain required | 89.64% | 98.33% |
| Tail mean first receipt wait (ms) | 67.754 | 75.389 |
| Within that wait: before sync (ms) | 0.074 | 24.864 |
| Within that wait: inside sync (ms) | 67.655 | 50.501 |
| Within that wait: after sync (ms) | 0.024 | 0.024 |
| Tail mean client acknowledgement (ms) | 143.024 | 155.961 |
| Tail own-group submission through publication (ms) | 68.694 | 75.754 |
| Remaining client duration, unattributed (ms) | 74.329 | 80.207 |
| Maximum positional journal write (ms) | 0.154 | 169.888 |

Tail rows use each arm's own raw P99 population. Wait partitions sum to the
first wait; sync is not added again. Requests share groups, so request-weighted
timings are not writer wall. The remaining client duration is reported without
assigning it to queueing. Client/server clock origins are never compared.
The writer census records `POST_DRAIN_ALL` as 25.76% and 28.60% of its accounted
iteration time, respectively; that is not an independently measured continuous
wall-clock denominator. The [W1/W2 warnings](wp749-writer-census-defects.md) apply.

All 502,346 measured requests join uniquely to their actual journal groups, with
zero missing or dropped evidence. Checkpoint counts, selection sizes and triggers
reconcile. Deduplicated sync counts, sums and maxima exactly match the journal
census; receipt-wait call counts match too. All-wait minus first-wait totals are
28 microseconds and 3 microseconds, respectively, from ready rechecks at census
resolution. This validates consistency, not observer overhead or host isolation.

## Why the comparison stopped

Both workload processes exited successfully with zero errors or unavailable
outcomes. Both preflights and the disabled postflight passed. The enabled
postflight observed two SSH authentication processes exiting during its
three-second sample. The retained system journal identifies an unauthenticated
login refusal and preauthentication close at 17:51:35 UTC. Another unauthenticated
connection occurred at 17:49:20–21 during the enabled invocation. The measurement
used one persistent monitoring connection throughout; these were separate probes.

Whole-cell CPU steal was zero and no process crossed the configured interference
threshold. Those facts do not override the stable-process-identity rule. The
postflight failure remains recorded; its timing is not silently excluded, and
the caller did not retry or use ADR-0143's replacement allowance automatically.

## Production wiring and next decision

The ordinary production daemon constructs `RunningArchiveWorkers` in
`ProductionGraphBuilder::build_inner`. The enabled worker advanced a validated
archive manifest beyond the stopped-backup frontier. The command measurements
surround generated-client calls through response receipt and decoding. The
storage path uses `RealJournalMedia`, the repaired receipt wait and the real
completion/publication path. An earlier local process proof checked those same
joins; test-only construction is not used as wiring evidence.

The worker receives published snapshot pins and has no source coordinator or
mutation-gate handle. Its file/directory durability work is outside source
acknowledgement ordering. That preserves the write-gate boundary; it does not
prove unchanged acknowledgement latency.

The [valid qualification comparison](wp749-archive-qualification-2026-09.md)
still records acknowledgement movement. This attempt adds an observed blocking
write path but cannot settle the archive-specific mechanism under the host rule.
No latency amendment is presented as justified by this invalid pair. A further
measurement would require a separate decision and isolation that addresses the
unauthenticated SSH probes; the current authorization ended with this attempt.

The quantitative bank is
`release/evidence/wp-749/archive-ack-attribution-20260921/README.md`.
Frozen base: `be189cac4325c77303c5b4fe749ef9b73f4e4b42`, plus the privately banked
observational patch. Rust 1.98.1, thin LTO, one codegen unit, generic x86-64;
8-vCPU AMD EPYC 9B14 with required hardware instructions. Both arms ran identical
daemon and runner digests. The full dataset, c32 write-only, Standard durability,
15-second warmup and 90-second measurement matched the prepared protocol.

## PR note

Package WP-749; tier surface for evidence paths under existing measurement ADRs.
Behavior: none; this report and the linked qualification page record the stopped
diagnostic and its limits. Checks: normal daemon/runner builds, probe tests,
production process proof, trace/counter reconciliation and documentation checks.
Compatibility: no public interface or durable-byte change. Hazards: host-invalid
enabled cell, unmeasured kernel/device cause and explicit client-duration
remainder. WP-749 remains open; capture representation work remains closed.
