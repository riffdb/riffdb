# WP-749 isolated checkpoint-wait pair

**Both cells are host-valid.** The bounded replacement pair confirms substantial
checkpoint-coupled journal waiting without archive. It also reproduces long
pre-sync positional journal blocking in the enabled arm. It does not certify
unchanged acknowledgement latency or close WP-749. See the full handbook report
at `docs/performance/wp749-archive-isolated-pair-2026-09.md`.

`summary.json` retains the exact frozen source and executable identities,
protocol, private-input digests, host checks, isolation/restoration proof hashes,
all raw percentiles, slowest twenty requests, checkpoint events, stage census,
request/group coverage, wait/sync reconciliation and explicit client-duration
remainder. The executed binaries are reused from the separately retained earlier
attempt; that attempt remains host-invalid. Neither raw host state nor private
connection information is published. Raw input and exported summary hashes are
distinct; `SHA256SUMS` authenticates the exported files.

The same analysis functions used for the earlier attempt are retained here.
Against a privately retained directory containing the two cell directories:

```sh
python3 compare.py /path/to/results
python3 reconcile.py /path/to/results/01-disabled
python3 reconcile.py /path/to/results/02-enabled
python3 trace_analysis.py /path/to/results/01-disabled
python3 trace_analysis.py /path/to/results/02-enabled
```

`compare.py` refuses host-invalid cells and mismatched executable identities.
`reconcile.py` deduplicates physical flushes and reconciles sync counts, sums,
maxima and receipt-wait calls. `trace_analysis.py` partitions the first wait
without adding overlapping sync time, and permits unique maximum-write assignment
only with one frame per flush and exactly one possible enclosing interval.
Exposure counts use unique groups represented in the measured request arrays;
the strict thresholds are 10,000,000 and 50,000,000 ns.

This was one fixed-order instrumented diagnostic, not a new performance
campaign. Both arms used identical binaries. Observer overhead, order effects,
kernel/device cause and the remaining client duration are not attributed.
Accounted writer iteration time is not independently measured continuous wall;
request-weighted shared group timings must not be summed as writer wall.
