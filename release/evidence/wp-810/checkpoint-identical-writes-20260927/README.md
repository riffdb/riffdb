# WP-810 byte-identical checkpoint finalization writes

The six-cell comparison is complete. The candidate shows a limited tail
benefit: median paired P99 **−9.30%**, with two substantial reductions and one
nearly flat pair. Throughput is unchanged within its recorded host variance;
P95 is mixed. Long journal waits remain. The implementation is retained for
maintainer review; no merge or package completion is recorded.

The rejected 8 MiB early-sync strategy remains closed. This candidate retains
all original durability fences and omits a finalization write only after
comparing every byte against the current backend. A fixed 4 KiB buffer and
shared mutation mutex bound the comparison and exclude competing writes,
resizes and syncs. Every growth and final sync remains unconditional. Extra
reads and backend serialization are the implementation's cost; there is no
cache, hash shortcut, codec, public knob or larger engine cache.

## Frozen comparison

- Control: `df7ad5b20231bcb64f402dc8c26cd51f8fe959d3`, including main
  `aa77f9622`; its runtime matches the restored control `983881ea`.
- Candidate: `f6b4315b8e73614bf6038d153d9f7ae2ca1c4d59`.
- Hardware: 8 vCPU AMD EPYC 9B14, with the required hardware instructions.
- Rust 1.98.1, thin LTO, one codegen unit, generic x86-64 target and unchanged
  server allocator. Distinct server digests and one common runner are retained.
- Full 19,220-command seed, c32 write-only, archive disabled, 15-second warmup
  and 90-second measurement. Fixed order: control/candidate,
  candidate/control, control/candidate. All six host/correctness checks pass;
  there are no retries, omitted cells, sample drops or reused sample arrays.

| Cell | Arm | Ops/s | P50 ms | P95 ms | P99 ms |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | Control | 3,200 | 7.989 | 11.375 | 99.018 |
| 2 | Candidate | 3,202 | 8.028 | 10.783 | 88.510 |
| 3 | Candidate | 3,152 | 8.021 | 11.161 | 100.492 |
| 4 | Control | 3,181 | 7.924 | 10.825 | 100.896 |
| 5 | Control | 3,160 | 8.053 | 10.884 | 96.733 |
| 6 | Candidate | 3,192 | 8.055 | 11.025 | 87.740 |

Median paired changes: throughput **+0.06%**, P50 **+0.48%**, P95 **+1.29%**,
P99 **−9.30%**. Per-pair P99 changes are −10.61%, −0.40% and −9.30%.
ADR-0239's 2–4% host variance is for throughput. It supplies no P95/P99 noise
band or statistical significance claim. This bounded comparison establishes
neither release qualification nor a predictable P99 reduction on other loads.

## Shared-wait evidence

| Pair | Mean journal sync, control → candidate (ms) | Change | Mean journal queue change | Mean receipt-block change |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1.600 → 1.505 | −5.95% | −6.20% | −4.88% |
| 2 | 1.562 → 1.593 | +1.94% | −0.24% | +1.64% |
| 3 | 1.537 → 1.494 | −2.78% | −6.38% | −2.73% |

Journal means use physical flush counts; queue and receipt-block means use
their own observation/call counts. These counters include warmup and overlap
writer time and each other. They are never summed into wall time.

Candidate peak journal syncs are 154.0, 210.0 and 169.1 ms, versus control
167.9, 152.9 and 165.0 ms. The shared long-wait mechanism persists; the candidate
does not eliminate it. The earlier native diagnostic identifies journal file
writeback and device flush during checkpoint commit. This light paired
observer cannot attribute each request to native I/O or quantify omitted
production write bytes, so it does not prove the cause of each changed percentile.

Every cell has 105–107 checkpoint starts, all byte-triggered, with zero
failures. Independent writer spans are approximately 105 seconds; signed
span-minus-iteration gaps are 16.16–18.33 ms and level-0 residuals 8.47–9.95 ms.
Busy plus idle undercounts those spans by 246.18–282.08 ms. `result.json`
retains each value separately, along with the slowest 20 raw requests per cell.

## Pinned-engine count reproduction

`redb-reproduction/src/main.rs` is the exact local observation program, using
the already pinned redb 4.2.0 with default features disabled. It uses a real
file and the production 4 MiB cache, forwards **all** original writes, and
checks every row through commit and reopen. `census.jsonl` retains all three
transactions; phase 1 is insertion/replacement and phase 2 is commit. The
result records the private harness manifest/lock digests without introducing
another workspace package. This is a count reproduction, not a timing result.

The three finalization phases contain 93.82%, 93.70% and 99.44% byte-identical
writes. That workload proves the pinned engine's redundant-write mechanism;
these percentages are not measurements of the production benchmark. Both
local and remote engine sources match the pinned crate archive. The production
path runs through the server's ordinary storage opener and asynchronous
checkpoint worker, with the scope immediately around its existing durable
commit. The comparison does not use a test-only alternate backend.

## Correctness and custody

The pass-through version first fails the new omission assertion. The candidate
passes nine targeted proofs, 1,046 storage tests, 294 coordinator tests, six
scoped acceptance steps and full CI. One pre-existing synthetic-history timing
generation test remains skipped; it is not a new correctness exclusion.
The new proofs include deterministic competing backend operations, changed
trailing bytes, error propagation, real-engine final-sync failure, and process
crashes before/after that fence with two exact reopens. Existing production
pipeline and recovery schedules pass with the actual wrapper installed.

Both exact remote-built servers and the common runner pass local full-workload
qualification before remote measurement. Local qualification overlaps CI and
has **no performance verdict**. The prospective protocol and gate receipts
remain unchanged; `checks.json` and `ci-result.json` record final results.
Nine unchanged client-only runner warnings are explained in
`runner-warnings.json`; none is suppressed.

All 125 remote evidence files passed local size/digest verification. All six
analyses and the complete comparison reproduce byte-for-byte from raw inputs.
Exact binaries, source bundle, raw samples, all attempts and private inventories
are retained outside this public repository. `retention.json` binds their
identities. Host isolation is restored, no task process remains, and 2.91 GB
of task-owned remote source/build/data/export files are removed. Local temporary
qualification sources and reproduction databases are removed too; shared build
cache and this unfinished worktree remain.

With the retained private evidence, reproduce the arithmetic using:

```sh
python3 analyze.py <retained-root>/results
python3 summarize_result.py <retained-root> result.json
python3 -m unittest test_evidence.py test_qualification_gate.py
```

`prototype.patch` retains the exact source/tests diff; the common observation
patch contains intentional single-space unified-diff context lines. Neither
patch is a generated change to the durable format. Final implementation/fixture
review and the shared-delay disposition remain the maintainer's decision.
WP-749's archive-presence gate remains separately unmet.
