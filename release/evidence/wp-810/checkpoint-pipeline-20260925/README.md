# WP-810 bounded checkpoint scheduling comparison

All six fixed cells are valid. Median paired changes are +5.06% throughput,
−2.80% P50 and −12.62% P95. P99 moves +23.40%, +9.92% and −12.33% across the
three pairs. There is no consistent acknowledgement-tail reduction and no
WP-810/WP-749 completion claim. See the
[handbook report](../../../../docs/performance/wp-810-checkpoint-journal-tail.md#bounded-comparison-result).

Control `d9ae2633` and candidate `66c394fc` use the same Rust 1.98.1 release
configuration and observation patch, distinct server binaries and one shared
client binary. Order is control/candidate, candidate/control, control/candidate.
Every cell uses c32 write-only, full dataset, 15-second warmup and 90-second
measurement, with archive disabled. No attempts were retried or discarded.

`summary.json` retains all six results, exact source/executable identities,
raw-array digests, slowest twenty samples per cell, host-validity proof hashes,
checkpoint counts, writer windows, corrected publication V2 waits, journal
timing, independent writer span and signed remainders. Journal and publication
timers overlap writer work and are not added to its wall time. The level-2
parent-mapping defect remains excluded from attribution. The complete private
archive contains 124 hash-verified files, including raw samples and binaries;
its hash is distinct from this sanitized export's hashes in `SHA256SUMS`.

The production census shows the expected scheduling change: `POST_DRAIN_ALL`
falls from about 24–25 seconds to 34–48 milliseconds per cell. Mean journal
queue time rises from about 23–25 µs to 984–1,051 µs per frame, and 155–168 ms
sync maxima remain. These are aggregate observations, not per-request causal
assignment of the P99 changes. The original positional-write stall remains
separate earlier evidence; this comparison does not establish its device cause.

## Reproduction and limits

`common-observation.patch` is a private diagnostic patch, applied identically
to both arms. It retains bounded raw client samples and independent writer
wall counters. It is not a production interface or a claimed instrument fix.
Its eight writer tests, two sample-retention tests and a local debug functional
check passed. Debug timings are not used as performance evidence.

Against the retained private result directories:

```sh
python3 analyze.py /path/to/results
```

The analyzer rejects missing samples, raw/histogram disagreement, mismatched
census populations, invalid signed level-0 reconciliation, host-invalid cells,
identical server digests or reused raw sample arrays. `summary.json` additionally
requires exact signed level-1 reconciliation. It retains both level-0 residuals
and level-1 unnamed time, rather than treating their parents as fully attributed.
Observer overhead is not separately measured. This bounded comparison is not a
complete alpha release sweep or an archive-presence qualification.

Newer main fixes the frozen candidate's two full-CI startup failures. Local
integration `fcc6e1183` passes 13 focused checkpoint/restart tests, but has no
performance measurement or completed full CI run. This bank remains tied to
the frozen revisions; it is not retagged to that integration. Host settings
were restored and task source/build/data directories removed after verified
local retention. The package remains open for maintainer disposition.
