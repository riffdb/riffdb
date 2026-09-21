# WP-749 bounded archive acknowledgement diagnostic

**Stopped; not a qualified pair.** Two cells executed; the enabled cell failed
host postflight. No retry occurred and WP-749 remains open. See the handbook
report at `docs/performance/wp749-archive-ack-attribution-2026-09.md`.

`summary.json` retains source, compiler, executable and raw-evidence digests,
both host receipts, workload outcomes, complete trace coverage, exact raw
percentiles, the slowest twenty requests, checkpoint events, stage distributions,
counter reconciliation and the explicit client-duration remainder. Raw process
logs, complete request arrays and executed binaries have verified private copies.
No host address, login identity, private path or raw authentication log is
published here. Original raw hashes and exported summary hashes are distinct.

Both workloads completed cleanly. The enabled postflight observed two SSH
authentication processes exiting; the private system journal identifies a
rejected unauthenticated connection. Zero whole-cell steal and no process above
the interference threshold do not override the process-identity failure. Another
unauthenticated connection occurred earlier in the enabled invocation. Impact
is not inferred from either observation.

`correlate.py`, `validate_cell.py`, `reconcile.py` and `trace_analysis.py` analyze
the privately retained per-cell inputs. `compare.py` is the publication-time
analysis successor: it explicitly refuses a host-invalid pair before calculating
differences. The executed runner already stopped at that same host failure;
the exact executed script digests remain in the summary. Running a trace
validator successfully does not grant host eligibility.

Reproduce against a private evidence directory containing `report.json`,
`run.log` and `raw/raw-request-tail.json` for each cell:

```sh
python3 correlate.py /path/to/cell
python3 reconcile.py /path/to/cell
python3 trace_analysis.py /path/to/cell
```

The counter check deduplicates physical flushes and exactly reconciles sync
counts, sums and maxima plus receipt-wait call counts. First-wait sums are
compared with all-wait sums; ready rechecks remain explicit. Wait and sync
overlap, and request-weighted group timings are not writer wall time.

The largest enabled positional-write counter maps to one group by interval
containment: exactly one submission-to-sync interval is long enough to contain
that maximum, with one command frame per flush. This identifies an observed
blocking write and checkpoint-conditioned wait. It does not establish the
operating-system cause or a clean archive-presence contrast.
