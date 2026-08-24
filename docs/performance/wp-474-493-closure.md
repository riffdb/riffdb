# WP-474 through WP-493 closure audit

This audit closes the command-layout and journal-overlay implementation cohort
against current `main`. It does not qualify the alpha release and does not
replace the evidence owned by `WP-623` and `WP-674`.

## Implementation receipts

- `WP-474` through `WP-484` retain their package-specific mechanics and
  same-host evidence under `docs/performance/wp-474-*.md` through
  `docs/performance/wp-484-*.md`.
- `WP-485` completed with the specified negative mechanics result; production
  authority did not change on the rejected state-bearing-segment design.
- `WP-486` passed its mechanics prerequisite and produced accepted ADR-0104.
- `WP-487` through `WP-489` shipped the frozen composite view, frame-first
  publication, deterministic suffix recovery, asynchronous checkpoints, and
  maintenance barriers in commits beginning with `5156e86c` through
  `351ae33a`.
- `WP-490` selected the journal-authoritative overlay in production. Its
  historical package-level 30-second tri-backend sweep is represented by
  `tri-20260818T172110Z` (merged-report SHA-256
  `36b11c69408744d2af8db47c91c4bad643fae15609f6e1c75ecb67dd2e035883`).
  The four RiffDB/safe-app throughput ratios were 1.19, 1.34, 1.04, and 0.98
  at 1, 8, 32, and 128 clients, with zero classified failures. The four-point
  arithmetic-mean ratio was 1.05. This receipt satisfies the historical
  package activation threshold, but its 30-second windows are not release
  evidence under the later PERF-018 rules.
- `WP-493` shipped in `0e3f56e4` and is frozen by the architecture check that
  forbids migration preflight from returning to checkpoint-only reads.

## Current-head verification

The repository-wide all-feature test suite, formatting, Clippy, generated
artifact, requirement-coverage, and handbook checks passed during this closure
campaign. The focused current-head checks also passed:

```text
cargo +1.97.0 test -p riffdb-storage-api --test composite_view_conformance
  4 passed

cargo +1.97.0 test -p riffdb-testkit --test contract_migration_gate_a --all-features
  passed rows=70; migration completed and preflight remained fail closed
```

Current storage architecture tests retain the exact overlay publication,
bounded merge, journal recovery, migration-preflight, pay-once validation, and
redaction pins. The storage recovery matrix retains journal-prefix ordering,
direct-barrier re-anchoring, checkpoint/reopen, torn-tail, and migration
suffix coverage.

## Performance-governance boundary

The package-local activation comparisons predate PERF-018 and later PERF-008
revisions. They establish why the production mechanism was retained; they do
not waive current cloud stability, unary, mixed-load, seed, or tail gates.
`WP-623` remains open for the final performance qualification, and `WP-674`
remains blocked pending the accepted stability-rule amendment. No package in
this closure audit makes an alpha-release performance claim.
