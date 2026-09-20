# WP-749: repair first-wait evidence before testing checkpoint attribution

Date: 2026-09-20. Status: instrument repair; checkpoint rerun pending.
WP-748 feature work remains paused. This does not close WP-749 or attribute its
P99 regression to checkpoints.

## Headline: the old receipt counter omitted the wait that matters

Publication V1 `receipt_block` observed the second wait on an already completed
journal receipt. The first completion-owner wait was uncounted. All previous
claims treating that counter as complete journal waiting are withdrawn. Other
timers retain their own meanings; none retroactively recovers the missing wait.
[W3](wp749-writer-census-defects.md#w3-publication-v1-misses-the-initial-journal-receipt-wait)
defines V2, its regression proof, and its overlap/snapshot limitations.

## Production capture caused the adjacent-commit difference

The adjacent comparison was parent `325513c18aeb51cb211551237cc0641045e99bfd`
against child `5a7b8054c14d06c308082a0ba920fdb10fb83748`. Here **capture means
production durable command-prefix mutation post-images**, required for exact
application-sequence restore. It does not mean enabling diagnostic observers.

The retained comparison protocol set `writer_capture_probes: false`. Its
supervisor cleared inherited `RIFFDB_`, `CARGO_` and `RUST*` settings before
installing the fixed measurement environment. The parent already supplies the
production-evidence-off control; no switch that disables required evidence in
production is introduced.

The adjacent diff for `crates/riffdb-storage-redb/src/journal.rs`,
`examples/app-baseline` and `benchmarks/run-app-baseline` is empty. Both arms use
the same journal sync implementation and timer. The reported +24.97% sync time
per physical flush comes from the journal thread's timer around `sync_data`,
divided by physical flush count. It does not pass through `receipt_block`.
Consequently the old adjacent comparison measures the production change,
including its additional bytes and admission accounting; it is not evidence
that diagnostic capture increased sync cost. It does not isolate copying from
the other work within that production commit.

The private retained protocol SHA-256 is
`c7fbc69274c10890db910d76855ea0b7e11adb4ede3cc2dbd9acb7ecb8bc5ffe`;
the supervisor SHA-256 is
`580302969304ed59460959000db0d74ac5cd2ba12d3d3b53e1101ab79390043a`.
These identify the historical four parent/child cells, not the forthcoming
repaired-instrument rerun. Its observer overhead has no independent on/off
verdict; the repair must be applied identically to both arms.

## The checkpoint threshold is still a hypothesis

Historical frame bytes per command were 3,759.964 / 5,508.341 (parent/child).
Dividing the 16 MiB checkpoint trigger by those means gives 4,462.1 / 3,045.8
commands. The conjecture is that the parent's 4,096-transition trigger wins,
while the child's byte trigger wins earlier. These are **estimates, not recorded
checkpoint sizes or counts**. Group boundaries, quiescence and physical extent
limits can change the selection.

One bounded parent/child rerun will retain actual checkpoint counts, selected
sizes and crossed limits, the repaired receipt counters, existing writer-batch
stages and raw request durations. Both binaries use Rust 1.98.1 and the same
instrumentation. A failed cell stops the comparison; every attempt is retained.
The counts can confirm or reject the threshold estimate. They cannot establish
which individual requests a checkpoint delayed. The checkpoint-installation
drain remains intact, and unexplained P99 movement remains blocking.

## The broader build failure belonged to the paused branch

Main builds all targets. The paused branch's `2d4816033` introduced the
`StoredAdministrationAuditRecordV1::Promotion` variant without updating two
service-audit test helpers: `tests/service_audit/control_plane_execution.rs` and
`tests/service_audit/service_audit_ordering.rs`. The earlier report's
`crates/riffdb-commit/tests/` path was wrong; Cargo includes these root tests
through a relative path.

Commit `84344254e` restores exhaustive matches in those two helpers. The paused
branch then passed `cargo check --workspace --all-targets --all-features` and all
20 tests in the affected suites. No promotion behavior was resumed. The receipt
repair is a separate branch from current main (`68f696b50`), which already
contains ADR-0250's `DefectScope` API; no obsolete branch rebase was attempted.

## Review note

- Package: WP-749 investigation; no completion claim.
- Tier: guarantee by touched paths; diagnostic repair within accepted semantics.
- Behavior: version complete receipt-wait diagnostics; count actual asynchronous
  checkpoint selection and lifecycle; distinguish old incomplete reports.
- Checks: deterministic missing-first-wait regression reproduced, then corrected;
  ready-recheck/error/noncommand coverage and parser version checks pass.
- Handbook: this page and the writer-evidence defect warning.
- Compatibility: existing durable bytes, public API and checkpoint decisions
  unchanged; diagnostic readers must recognize the V2 publication line.
- Hazards: overlapping wait sums, independent atomic snapshots, W1/W2 wall-time
  defects, and aggregate-only checkpoint attribution remain explicit limitations.
