# WP-749 P99 arithmetic on the isolated pair

**Review evidence; no causal archive residual identified and no closure.**
See `docs/performance/wp749-archive-p99-decomposition-2026-09.md` for the
interpretation and `docs/performance/wp-806-checkpoint-journal-tail.md` for
independent ownership of the shared checkpoint mechanism.

`decomposition.json` binds the original summary and raw inputs by SHA-256. The
source remains `archive-ack-isolated-20260921/summary.json`; this is not another
run. `decompose.py` refuses changed inputs, invalid host receipts, mismatched
executables, duplicate sequences and intervals outside their requests.

Two distinct descriptive calculations are retained:

1. Subtract the actually observed own-group pre-sync wait from each request,
   then re-sort and calculate P99 in both cells. The contrast falls from
   26.582760 ms to 11.686830 ms, a 14.895930 ms change. Other interval
   subtractions are retained to make rank changes and their limits explicit.
2. Keep each arm's two empirical conditional request distributions and replace
   only the fraction whose own group requires checkpoint drain. Four weighted
   P99 values retain both factor-change orders. Exposure contributes 2.693300
   or 3.883560 ms; conditional latency change contributes the complementary
   23.889460 or 22.699200 ms. The equal-order averages are 3.288430 and
   23.294330 ms. Integer masses preserve exact empirical quantile boundaries.

Neither is a counterfactual runtime measurement. Their components are not
disjoint and must not be added together. The broader shared mechanism may
change severity or propagate through earlier groups. The remaining contrast
is not a proven archive-specific cost or a bound on one. The JSON leaves
`causal_archive_specific_residual_ns` null deliberately.

Recompute from the repository root against the privately retained cells:

```sh
python3 release/evidence/wp-749/archive-p99-decomposition-20260921/decompose.py \
  /path/to/retained/results \
  --source-summary release/evidence/wp-749/archive-ack-isolated-20260921/summary.json \
  --output /tmp/wp749-p99-recomputed.json
python3 -m unittest discover \
  -s release/evidence/wp-749/archive-p99-decomposition-20260921 \
  -p test_decompose.py
```

The ten semantic tests include a counterexample to subtracting percentiles,
pure exposure and pure conditional-distribution changes, unequal population
sizes, an exact 99% mass boundary, disjoint wait partitions, and refusals of
tampering, host-invalid input and invalid intervals. `SHA256SUMS` covers the
public artifacts. Raw private paths, process logs and machine addresses are
not included.
