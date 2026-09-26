# Retained checkpoint/sync overlap

This is a reanalysis of the already qualified WP-749 isolated pair at
`release/evidence/wp-749/archive-ack-isolated-20260921/summary.json`. It performs
no new workload and is not a measurement of WP-810's pipeline candidate.

The analyzer verifies all four consumed private files in each cell against
the original public bank's hashes. It checks final trace coverage, checkpoint
completion, monotonic group coverage, and shared-flush timestamp consistency.
It selects groups intersecting the measured command-sequence range and counts
each physical flush once, even if several groups share it. A boundary group can
include warmup commands. Intervals are half-open on one server clock; no client
and server clock comparison, percentile subtraction, or causal assignment is
performed.

| Journal sync observation | Archive disabled | Archive enabled |
| --- | ---: | ---: |
| Measured physical flushes | 16,607 | 15,198 |
| Over 10 ms, overlapping materialization | 158 / 158 | 173 / 173 |
| Over 50 ms, overlapping materialization | 123 / 123 | 116 / 116 |
| Over 10 ms, extending past materialization | 0 | 1 |
| Maximum sync with no registered checkpoint overlap | 3.917 ms | 2.521 ms |

Here **materialization** means checkpoint registration to the recorded
successful materialization result. It includes thread dispatch, transaction
begin, retained mutation replay and redb durable commit. The trace cannot split
those components. **Post-materialization** means completion to the recorded
installation result; it is not a measurement of an individual installation
syscall. All 158 disabled slow syncs finished before their checkpoint
materialized. The worst, 159.922 ms, finished 1.967 ms before that event.

This narrows the observed association to work before installation. It does not
show whether redb page writes, redb sync, filesystem scheduling, device queues,
or another unobserved cause delayed the journal syscall. It cannot explain the
new candidate's exact P99 movement. That requires observations on that source.

`summary.json` retains every sync over 10 ms and its overlapping checkpoint
boundaries, alongside aggregate counts. No raw logs, host identities, paths,
application values or binaries are exported. `SHA256SUMS` binds these artifacts;
the summary separately binds the original public bank and private inputs.

`qualification.json` separately retains the current integration's CI outcome
and its clean-main reproduction. That verification concerns `a8b3737d9`, not
the historical source used for the overlap calculation. Full CI stops at the
same seeded-corpus coverage assertion on both revisions; it is not waived.

Reproduce with the verified private results directory:

```sh
python3 release/evidence/wp-810/checkpoint-overlap-20260926/analyze.py \
  PRIVATE_RESULTS_DIRECTORY \
  release/evidence/wp-749/archive-ack-isolated-20260921/summary.json
python3 -m unittest discover \
  -s release/evidence/wp-810/checkpoint-overlap-20260926 -v
```

The tests check physical-flush deduplication, measured-range selection,
half-open boundaries, crossing multiple phases with a disjoint remainder, and
refusal of inconsistent flushes or incomplete/overlapping checkpoints.
