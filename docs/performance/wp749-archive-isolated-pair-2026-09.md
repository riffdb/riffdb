# WP-749: isolated checkpoint-wait comparison

**Checkpoint-coupled journal waiting exists with archive disabled.** In the
disabled cell, 123 measured groups waited more than 50 ms for their first
journal receipt; every one had the actual checkpoint drain predicate set.
Archive does not introduce this blocking dependency. The existing storage
checkpoint/journal path owns it, including the checkpoint-installation guarantee
that the drain preserves.

The single authorized replacement pair passed all host and correctness checks.
It also reproduced substantially longer positional journal writes with archive
enabled. This distinguishes a shared blocking path from an additional observed
archive-associated cost; it does **not** establish unchanged acknowledgement
latency. The bounded investigation is finished. No additional run, runtime
change, latency amendment or WP-749 completion is made by this report.

## Direct answer from the correlated requests

| Observation | Archive disabled | Archive enabled |
| --- | ---: | ---: |
| Successful measured commands | 263,888 | 240,636 |
| Raw acknowledgement P50 (ms) | 9.448 | 9.934 |
| Raw acknowledgement P95 (ms) | 11.729 | 13.028 |
| Raw acknowledgement P99 (ms) | 88.347 | 114.930 |
| Throughput (ops/s), disclosure | 2,932 | 2,674 |
| Measured groups with first receipt wait >50 ms | 123 | 163 |
| Those groups without checkpoint drain | 0 | 0 |
| First receipt wait P99, drain required (ms) | 56.855 | 62.228 |
| First receipt wait P99, no drain required (ms) | 1.068 | 1.582 |
| Requests whose own group requires drain | 66.97% | 68.21% |
| P99-tail requests whose own group requires drain | 95.72% | 97.17% |
| Checkpoints, including warmup | 98 | 90 |
| Mean checkpoint registered duration (ms) | 824.837 | 911.360 |
| Maximum positional journal write (ms) | 0.727 | 141.387 |

All 188 checkpoint selections were byte-triggered, at 3,008–3,327 transitions
off and 3,003–3,349 on. None reached the 4,096 transition trigger. Measured
groups exclude groups represented only in warmup; checkpoint counts include
warmup and the final completion of already-registered checkpoints.

The production path is unchanged: a registered async checkpoint requires
`POST_DRAIN_ALL`; journaled groups wait for the journal receipt and publication
before command completion. Disabled-cell sync reached 159.922 ms, demonstrating
that substantial checkpoint-coupled waiting does not require an archive worker.
Higher enabled drain exposure and checkpoint duration are consistent with
amplification of that shared path, rather than introduction of the dependency.

### The pre-sync write finding survives isolation

The enabled cell's **141.387 ms** maximum positional write maps uniquely by
interval containment to group 12,256, sequences 212,986–213,004, while checkpoint
61 is registered and drain is required. Its submission-to-sync interval is
141.467 ms, first receipt wait **142.265 ms**, and sync only **0.799 ms**.
The nineteen corresponding measured acknowledgements take 160.705–162.021 ms.
One command frame per flush and the absence of any other sufficiently long
enclosing interval make this assignment unique. The disabled 0.727 ms maximum
has five possible enclosing groups and is not assigned to one.

The earlier [169.888 ms write inside a 197.502 ms receipt wait](wp749-archive-ack-attribution-2026-09.md#observed-blocking-path)
remains a valid intra-run observation from the host-invalid attempt. The clean
replacement independently reproduces this blocking location in the enabled arm.
Neither run instruments the kernel/device cause of the long write. In
particular, this is not proof of dirty-page throttling, device contention, or a
new archive acknowledgement dependency.

## Accounting limits

| Own P99-tail population, mean duration (ms) | Disabled | Enabled |
| --- | ---: | ---: |
| Client acknowledgement | 142.197 | 150.456 |
| Own group submission through publication | 69.925 | 69.729 |
| Explicit remaining client duration, unattributed | 72.272 | 80.727 |
| First receipt wait | 69.351 | 69.319 |
| Within that wait: before sync | 0.077 | 17.918 |
| Within that wait: inside sync | 69.247 | 51.377 |
| Within that wait: after sync | 0.027 | 0.024 |

Each column selects its own P99 population. These means do not decompose the
between-arm P99 change. Wait partitions tile the first wait; sync must not be
added again. Shared group durations weighted by requests are not writer wall.
The client-duration remainder is not assigned to queueing or archive work.
Client and server clock origins are never compared.

All **504,524** requests join uniquely to actual groups, with zero missing or
dropped evidence, errors or unavailable outcomes. Deduplicated flush sync
counts, sums and maxima reconcile exactly with the journal census, as do receipt
wait call counts. All-wait minus first-wait totals are 28 and 51 microseconds,
respectively, from ready rechecks at counter resolution.

`POST_DRAIN_ALL` is 26.34% off and 28.35% on of accounted writer iteration time.
That denominator is not independently measured continuous wall; the
[writer-census warnings](wp749-writer-census-defects.md) remain applicable.

## Protocol, isolation and retained evidence

Exactly two cells ran, in disabled/enabled order, using the **same previously
built daemon and runner**, with their digests checked before and after each cell.
Frozen source is `be189cac4325c77303c5b4fe749ef9b73f4e4b42` plus the same bounded
observational patch used in the earlier attempt. This is not a build of later
main. Rust 1.98.1, thin LTO, one codegen unit, generic x86-64; 8-vCPU AMD EPYC
9B14 with the required hardware instructions. Full dataset, c32 write-only,
Standard durability, 15-second warmup and 90-second measurement per cell.
Observer overhead and fixed-order effects are unmeasured; these cells explain
paths and do not replace the five-pair production qualification.

Standing host permission covered a temporary operator-only SSH allowlist and
stopping OSConfig and apt/certificate/motd timers. Existing firewall rules and
unit enablement were preserved; automatic rollback was armed first. One
persistent monitoring connection was used throughout measurement. The temporary
rule dropped 21 IPv4 packets. Both preflights and both postflights passed,
including whole-cell validity. Prior host settings were restored and verified,
temporary source/database directories removed, and the exclusive host released
at **19:13:47 UTC on 2026-09-21**.

The enabled production worker advanced from backup sequence 19,220 to archive
sequence 305,505 with a validated manifest digest and no observed terminal
failure. This proves worker execution, not caught-up status. The production
wiring audit in the earlier report continues to apply to these identical
executables.

`release/evidence/wp-749/archive-ack-isolated-20260921/README.md` retains the
redacted quantitative bank, exact source/binary/raw-input digests, slowest twenty
requests, checkpoint events, full stage census and reproducible analyzers. Raw
logs, complete sample arrays and isolation receipts have verified private copies.
The earlier failed attempt remains separately retained and host-invalid.

## Gate and ownership handoff

The common checkpoint durability/publication drain belongs to the storage and
journal path. Archive is not its origin, and removing that drain would require
preserving its checkpoint-installation guarantee elsewhere. No such change is
proposed here.

The isolated enabled arm has P50 +5.15%, P95 +11.07% and P99 +30.09%, with
throughput -8.80% disclosed separately. Together with the
[five-pair qualification](wp749-archive-qualification-2026-09.md), this still
does not support the literal acknowledgement-latency parity claim. Presence of
the shared mechanism does not explain away the additional archive-associated
pre-sync blocking or establish its resource-level cause. ADR-0249 supplies no
numeric P99 rejection threshold; this report neither invents one nor certifies
the entire incremental latency movement as explained.

The next maintainer decision is how to handle that incremental latency evidence
separately from ownership of the baseline checkpoint mechanism. WP-749 is not
closed against a baseline storage issue, and a follow-up owner's name is not
substituted for a mechanism or an accepted latency-clause amendment. This report
ends the authorized measurement work; it does not open another hypothesis run.

## PR note

Package WP-749; tier surface for evidence under existing measurement ADRs.
Behavior: none. Updated handbook pages distinguish the shared checkpoint path,
additional observed enabled blocking, and the bounded diagnostic's limits.
Checks: same executable/source digests, four host checks, workload correctness,
complete request/group/checkpoint reconciliation and documentation acceptance.
Compatibility: no runtime, public interface or durable-byte change.
Hazards: fixed order, observer overhead unmeasured, kernel/device cause and
client-duration remainder unattributed. No merge or completion claim.
