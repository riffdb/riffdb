# WP-810 full-workload diagnostic capacity

This is the qualified replacement for the undersized checkpoint-I/O probe.
The earlier source, binaries, 4,194,304-record patch and failed evidence remain
in `../materialization-probe-20260926/` and
[`../materialization-overflow-20260926.json`](../materialization-overflow-20260926.json).
No production implementation or durable format changes here.

## Capacity and scope

The failed full remote cell attempted 9,442,900 I/O observations. This probe
raises the fixed limit to 16,777,216: 1.777 times that observed population.
The fixed record-memory ceiling rises from 352 to 1,312 MiB; the log ceiling
rises from 768 MiB to 2 GiB, with at least 3 GiB free in the private tmpfs spool
required before launch. Groups remain bounded at 65,536, checkpoints at 512
and each emitted page at 32,768 records. Raw requests keep their existing
32-worker / 32,768-samples-per-worker bound. Every overflow still invalidates
complete attribution; no rows are sampled, overwritten or silently discarded.

The compiled observation patch differs from the earlier patch only in the
I/O-record limit and its memory-ceiling assertion. It still observes the
production redb file backend with unchanged operations, ordering and error
handling. The new memory footprint and observer overhead preclude treating
these diagnostic timings as an uninstrumented baseline or mitigation comparison.

## A full qualification is mandatory

`qualify_full.py` uses the full 19,220-row dataset, c32 write-only load, a
15-second warmup and a 90-second measurement, with a fixed 900-second cell
deadline. It copies RAM output even on failure. It requires complete traces,
all request/flush joins, checkpoint and journal-census reconciliation, and the
independent writer-wall checks before writing a qualification receipt.
It makes no qualified performance claim about the local host.

`build_then_gate.py` verifies the source, observer and both executable digests,
then requires that full-capacity receipt before the remote cell. The receipt
must name the full workload, carry the full-scale report identity, retain at
least 89 seconds between measured sample boundaries for the requested 90-second
window, and report completed, positive, matching row populations without drops.
A boolean-only qualification, smoke dataset, short run or previous smaller
capacity cannot open this gate. Tests demonstrate that the legacy gate admitted
16 such invalid cases, and the new gate refuses them.

`measurement-protocol.json` freezes the prospective one-cell diagnostic.
Local full-workload qualification must finish before a new host reservation
is proposed. Rebuilt remote binaries must pass that same full qualification
locally, by exact digest, before remote measurement starts. A failed attempt
is retained and stopped; shortening the qualification is not a substitute.

## Current status

Five collector and two backend checks pass, as do 24 analyzer, identity, gate
and retention tests. The full local qualification passes across 357,879
measured requests, 139 checkpoints and 12,924,427 I/O records, without drops.
All joins, phase bounds, journal counters and signed wall reconciliation pass.
The 998,090,680-byte log also exceeds the old 768 MiB log ceiling and fits the
new 2 GiB ceiling. The trace exercises 3,481,527 more records than the failed
remote cell, leaving 3,852,789 entries unused in the new fixed buffer.
[`qualification.json`](qualification.json) records the exact binaries and proofs.
The authorized remote rebuild then passed full local qualification with its
exact executable digests: 328,819 measured requests and 11,801,889 I/O rows,
without drops. The single remote diagnostic subsequently completed 282,003
measured requests, 105 byte-triggered checkpoints and 9,431,998 I/O rows with
complete joins, phase bounds, counter reconciliation and valid host checks.
There was no retry. All 50 retained files were verified locally; host isolation
was restored and task-owned remote build/data directories were removed.

[`result.json`](result.json) records identities, qualification, raw percentiles,
counts, phase/backend distributions, timing limits and private-evidence digests.
[`derived-result.json`](derived-result.json) narrows the remaining wait: all 43
measured journal syncs above 100 ms occur entirely inside checkpoint durable
commit and a checkpoint backend sync. A 156.209 ms journal sync sits inside a
156.302 ms receipt wait. Without a registered checkpoint, journal sync peaks
at 3.056 ms. This is same-clock association, not kernel/device causation.

Reproduce the derived result from the retained validated cell with:

```bash
python3 summarize_result.py <private-cell-directory> derived-result.json
```

The script checks the trace digest, coverage and measured-flush distribution
before deriving witnesses. Request/server clocks are never subtracted.
The complete [performance report](../../../../docs/performance/wp-810-checkpoint-journal-tail.md#complete-replacement-journal-sync-overlaps-checkpoint-commit-sync)
preserves the distinction between this diagnostic and the earlier comparison.
WP-810 remains open pending a reviewed resolution of the shared delay. No
additional measurement or completion is authorized by these results.
