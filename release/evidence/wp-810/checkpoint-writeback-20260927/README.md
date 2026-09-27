# WP-810: reject the 8 MiB checkpoint early-sync prototype

Three fixed counterbalanced pairs all regress. The prototype is withdrawn;
the runtime is restored to control `983881ea`. WP-810 remains open, and
WP-749's archive gate remains separate. No budget tuning or additional run
follows this result.

## Frozen comparison

- Control: `983881ea28746b2e9fdb4bf20ff45d12c6d01f53`, including main through
  `aa77f9622`.
- Prototype: `d47351466a276ba14c9a3a136e6ae586d2d24d2b`.
- Withdrawal: `0e85750c98f87b7386f30db130afa9c21f6cc595`.
- Hardware: 8 vCPU AMD EPYC 9B14 with the required hardware instructions.
- Rust 1.98.1, thin LTO, one codegen unit, generic x86-64 target, unchanged
  server allocator. Distinct server digests and one common runner are retained.
- Full 19,220-command dataset; c32 write-only; archive disabled; 15-second
  warmup and 90-second measurement. Order: control/candidate,
  candidate/control, control/candidate. All six host and correctness checks pass.

The internal prototype synced the primary redb backend before more than 8 MiB
of checkpoint writes could accumulate between successful syncs. It kept the
single transaction, growth syncs, final Immediate commit sync, exact checkpoint
installation, acknowledgement ordering, and every existing capacity bound.
`prototype.patch` retains the exact implementation and tests; it is **not
selected production behavior**. No durable format or public setting changes.

## Result

| Cell | Arm | Ops/s | P50 ms | P95 ms | P99 ms |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | Control | 3,160 | 7.899 | 10.843 | 99.552 |
| 2 | Prototype | 1,024 | 9.390 | 130.553 | 133.964 |
| 3 | Prototype | 1,025 | 9.470 | 130.180 | 133.720 |
| 4 | Control | 3,182 | 7.929 | 10.611 | 100.809 |
| 5 | Control | 3,166 | 8.047 | 10.785 | 94.433 |
| 6 | Prototype | 1,021 | 9.684 | 129.624 | 133.270 |

Median paired changes: throughput **−67.75%**, P50 **+19.42%**,
P95 **+1,104.08%**, P99 **+34.57%**. Raw percentiles are used; the six sample
arrays are distinct and reconcile with their histograms and request counts.
All three pairs agree on the direction. This is a bounded comparison, not
alpha release qualification.

The smaller maximum journal sync does not make the prototype useful. The
control's maximum is 156.5–167.7 ms and the prototype's 128.1–128.8 ms, while
mean journal sync per physical flush rises from 1.54–1.62 ms to 10.12–10.44 ms.
Cumulative journal sync grows from 32.58–34.18 seconds to 80.10–81.78 seconds
over each roughly 105-second writer interval, despite much less completed work.
The added checkpoint syncs aggravate the shared durability wait.

The comparison does not retain native checkpoint I/O intervals or device byte
counts. It cannot establish page-write coalescing loss or select another pacing
budget. Writer/checkpoint/journal counters include warmup; client percentiles
and throughput are measured-only. Journal and publication timers overlap each
other and writer time. `result.json` retains signed independent-wall and
level-0 reconciliation for every cell; existing W2 limitations remain.

## Correctness, reproduction and custody

The prototype passes 1,045 storage tests, 294 coordinator tests, seven scoped
acceptance steps and full CI. Its full CI log reports 6,157 passes across 556
result groups, including repeated validation and recursive crash children;
that is not a unique-test count. Full-workload local qualification passes for
both exact remote-built servers and the common runner, with zero dropped
samples. Local timings are not performance evidence.
After withdrawal, 16 targeted checkpoint, online-retention and exact-receipt
tests pass on the restored control; `withdrawal-check.json` records that check.

`qualification.json`, `ci-result.json`, `source-freeze.json` and the original
prospective `measurement-protocol.json` retain the identities and gates.
The common observation patch retains bounded raw requests and an independent
writer span; the heavy backend/native diagnostic is not in these binaries.

`result.json` is a whitelisted report. All 126 remote evidence files passed
local size/digest verification. Recomputing the complete comparison from raw
requests and logs produced byte-identical JSON. Original logs, binaries, source
bundle and private host inventory remain outside this public repository.
Host isolation was restored and approximately 2.97 GB of task-owned remote
build, source, database and export files were removed. No task processes remain.

With the retained private result directory, reproduce the arithmetic using:

```sh
python3 analyze.py <retained-results-directory>
python3 -m unittest test_evidence.py test_qualification_gate.py
```

The qualification gate rejects a missing arm, stale/same server, changed
runner/source, shortened workload, dropped samples or incomplete validation.
No merge, package completion, archive-gate pass or effective mitigation is
claimed by this negative result.
