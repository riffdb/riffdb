# WP-430 through WP-448 closure audit

This audit closes the columnar projected-read, reactive repair, realistic
benchmark-harness, and storage-hygiene cohort against current `main`. It does
not claim that the current alpha release matrix is qualified.

## Implementation receipts

- `WP-430` shipped the columnar core in the CP1 merge `47f626a7`.
- `WP-431` and `WP-432` shipped the engine surface and public acceptance spine
  in `0cc5250d` and `4184b171`.
- `WP-433` and `WP-434` shipped the projected board benchmark and generated
  SDK/CLI/packed carriage in `fc0b78f4` and `208a355d`.
- `WP-435` and `WP-436` repaired reactive publication audit linking and
  idempotent republish parity in `73a15cf1` and `01f28c34`.
- `WP-437` through `WP-445` shipped together in `a5a0cde3`. The current
  app-baseline self-test still exercises repetition stability, semantic
  reconciliation, exact comparator identities, open-loop accounting, bounded
  shape matrices, dead-peer abort, typed outcomes, and evidence eligibility.
  These packages define and verify the harness; they do not mark a release
  matrix eligible when required evidence is absent.
- `WP-446`, `WP-447`, and `WP-448` shipped the write-lock-free reactive
  republish lookup, single-pass startup verification, and constant-time
  shutdown checkpoint construction in `abadb5e2`, `163bf294`, and `66966813`.

## Current-head verification

The closure campaign ran the repository-wide all-feature test suite, formatting,
Clippy, generated-artifact, requirement-coverage, and handbook checks. It also
ran:

```text
cargo +1.97.0 test -p riffdb-columnar -p riffdb-projection --all-features
  riffdb-columnar unit: 36 passed
  columnar acceptance: 25 passed
  CP2a: 11 passed
  projection and result-provider suites: all passed

./benchmarks/run-app-baseline --self-test
  app-baseline suites: all passed, including dead-peer abort
```

The current projected-read tests retain independent-oracle equality,
organization isolation, exact frontier behavior, checkpoint/recovery,
compaction, corruption refusal, result-window bounds, and all-or-none
multi-entity publication. Current benchmark tests retain the distinction
between harness validity and release-evidence eligibility.

## Release boundary

Closing this cohort records implemented behavior and current regression
coverage. `WP-578`, `WP-579`, `WP-623`, and `WP-674` remain open; no historical
benchmark-framework package is used to bypass their endurance, deployment,
cloud-performance, or stability evidence.
