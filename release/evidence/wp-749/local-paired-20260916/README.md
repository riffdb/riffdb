# WP-749 local paired write-path disclosure

Package: WP-749 (open). Tier: surface (the release/** path rule); implementation evidence under accepted ADR-0178.
Measured 2026-09-16. Source behavior and guarantees are unchanged by this bundle.

## Result

All four cells exited successfully, with clean correctness, reported-valid pre/postflight
host observations, zero whole-cell steal, and stable three-generation results
under the existing harness rules. These checks do **not** establish no regression.

**Host visibility correction:** These reports do not bind process-namespace
visibility. Their empty active-process inventories cannot establish host-wide
absence of interference. A later bounded observation confirmed that the ordinary
tool sandbox exposes only its own processes; the host-visible sampler refused a
new run because of unrelated activity. See `../host-observation-20260916/`.
Retain the original numbers and raw flags as unqualified local observations;
actual interference during these earlier cells is unknown. This correction
changes no raw report, log, execution receipt, protocol, or measured ratio.

| Cell | Median ops/s | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: |
| Historical prefix control | 3,844 | 8.13 | 11.01 | 20.97 |
| Current prefix implementation | 3,687 | 8.39 | 11.53 | 37.75 |
| Current, archive disabled | 3,674 | 8.39 | 11.53 | 37.75 |
| Current, archive enabled | 2,860 | 11.01 | 17.83 | 32.51 |

Current versus historical: throughput -4.08%, p95 +4.76%, p99 +80.00%.
Enabled versus disabled: throughput -22.16%, p95 +54.55%, p99 -13.89%.
The archive comparison shows a material cost and does not satisfy the package's
no-regression exit gate. Neither the measurement nor this disclosure adds a
threshold, admits a performance candidate, lifts the freeze, or closes WP-750.

The enabled generations confirmed archive application frontiers 39,791, 39,609,
and 39,480 from backup frontier 19,220; their history-transaction advances were
1,567, 1,587, and 1,589. All terminal manifests validated and no terminal collector
failure was observed. Each source generation committed approximately 299,000
commands including warmup, so collection lagged substantially. Shutdown cancels
the collector; this evidence explicitly makes no full-catch-up claim. Disabled
generations created no archive and report absent progress and manifest identity.

## Protocol and identities

`protocol.json` was written before any cell. The fixed order is the table order;
no cell was retried. Each used the full dataset, write-only load, 32 clients,
three independent 90-second measured generations and 15-second warmups, the
standard storage profile, and the existing `--require-stable` wrapper. PostgreSQL
was omitted, so raw `comparison_complete` is false. This is one local workload
point, not interactive/load-sweep, comparator, repeated-kill, or N1/E2 qualification.

Host: AMD Ryzen 9 7950X, 32 logical CPUs, Gentoo, Samsung SSD 990 PRO 2TB,
ext4, powersave governor. The resource summaries retain their original process
scope, which includes warmup; they are not measured-interval-only resource rates.

The historical daemon source is `a7e2088bdd84375412d17a519eccfefaa11256db`,
the parent of atomic prefix capture. Current daemon and harness source is
`6d2d00d6573499dce1b5fa4743d57abaad2630d8`. Both use the same current runner.
The shell wrapper bytes are identical across source checkouts. The historical
comparison includes subsequent implementation changes and does not isolate one
patch's causal cost. The historical binary is only a test control.

`identities.json` binds source revisions, root/nested Cargo.lock hashes, harness
tree, and copied daemon/runner hashes. Every cell checked clean source and exact
binary hashes before launch. Raw reports independently bind their source,
wrapper, runner, daemon, root lock and host observations. In the historical cell,
the report's source revision identifies the historical daemon checkout; the
protocol additionally identifies the current runner's actual source and nested
lock. `run.py` is the exact predeclared driver, including its original local paths.

## Reproduce the summary and check retained bytes

From this directory:

```sh
sha256sum --check SHA256SUMS
python3 summarize.py > /tmp/wp749-local-summary.json
cmp summary.json /tmp/wp749-local-summary.json
```

The summarizer validates source/binary identities, execution success, correctness,
host validity, generation shapes, archive mode and progress, then computes ratios
from raw medians. It never reruns the workload or applies a new comparison gate.
Raw reports, complete logs, execution receipts, protocol and original driver are
retained together. `SHA256SUMS` covers every file except itself.

## Checks, compatibility, and follow-ups

The actual four-cell run supplies the measurement checks. Summary recomputation
and checksums reproduce from the copied reports. Documentation/governance checks
are recorded in the commit note; this evidence commit changes no runtime or wire
format and does not claim full merge CI or package closure. The affected handbook
page is `docs/performance/app-baseline-alpha.md`.

Investigate collector lag and the observed latency/throughput cost with focused
correctness and profiling evidence before any new comparison. Preserve these
results and their protocol. Complete the remaining WP-749 crash/exact-stop
qualification and full CI, WP-748 administration/promotion, and WP-750's required
N1/E2 campaign. Within-cell stability must never be reported as cross-cell parity.
