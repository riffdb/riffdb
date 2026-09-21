# WP-749: repair first-wait evidence before testing checkpoint attribution

Date: 2026-09-20. Status: instrument repair passed acceptance; the remote
checkpoint rerun stopped at host preflight before either measurement. Follow-up
forensics distinguish that observation from a daemon failure. A local counter-only
exercise subsequently retained 12 actual byte-triggered checkpoint selections.
At that checkpoint, WP-748 feature work remained paused and the tail was
unattributed. The final disposition below supersedes that investigation status.

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
the other work within that production commit. **The approximately 25% sync
increase is a production-capture cost result, not tail-mechanism attribution.**
The feature independently changes the measured I/O cost. This comparison cannot
attribute P99 movement to checkpoint waiting, and the historical parent/child
tail comparison is withdrawn. A causal comparison must hold production capture,
baseline, toolchain and observers constant except for the mechanism under test.

The private retained protocol SHA-256 is
`c7fbc69274c10890db910d76855ea0b7e11adb4ede3cc2dbd9acb7ecb8bc5ffe`;
the supervisor SHA-256 is
`580302969304ed59460959000db0d74ac5cd2ba12d3d3b53e1101ab79390043a`.
These identify the historical four parent/child cells. Repaired observer overhead
has no independent on/off verdict; the repair must be applied identically to
both arms of any future timing comparison.

## Historical threshold estimate and actual local counts

Historical frame bytes per command were 3,759.964 / 5,508.341 (parent/child).
Dividing the 16 MiB checkpoint trigger by those means gives 4,462.1 / 3,045.8
commands. The conjecture is that the parent's 4,096-transition trigger wins,
while the child's byte trigger wins earlier. These are **estimates, not recorded
checkpoint sizes or counts**. Group boundaries, quiescence and physical extent
limits can change the selection.

The checkpoint census records actual selections, sizes and crossed limits.
Counts can test the threshold estimate; they cannot establish which requests a
checkpoint delayed. The checkpoint-installation drain remains intact, and
unexplained P99 movement remains blocking. The earlier historical binaries will
not be reused for tail attribution: they also predate ADR-0250's defect breaker.

### Bounded attempt: no checkpoint measurements collected

The two repaired daemons and shared raw-sample runner built successfully on the
8-vCPU AMD EPYC 9B14 host with the required hardware accelerators. The build
receipt verifies distinct daemon digests, identical instrument sources and
lockfiles, and Rust 1.98.1. All seven local acceptance steps passed first,
including **1,539 tests** (nine skipped by the existing profile), scoped Clippy,
formatting and handbook checks. The workspace all-target/all-feature build,
instrument regression tests and 26 benchmark-runner tests also passed.

The first cell's three-second host preflight then returned `valid: false`,
`reason: host_interference`: **one process identity disappeared between sampled
inventories**. It
reported no active or interfering processes, no started processes or PID reuse,
and zero steal ticks. This receipt does not identify the exited process or
establish resource contention or distinguish an actual exit from an unavailable
`/proc` identity read; it does fail the declared stable-process check.
The supervisor stopped before launching the benchmark. Neither measurement
cell started, and there was no retry.

The [retained attempt summary](https://github.com/riffdb/riffdb/blob/main/release/evidence/wp-749/checkpoint-counts-20260920/summary.json)
binds the binary and preflight digests. **That attempt contains no checkpoint
count or new latency verdict.** The threshold estimate remains unverified by
those historical binaries; the failed preflight remains in the record.
All 33 retained files, including the three frozen binaries, have verified local
second copies. The remote source checkouts were removed and the host released.

### Exit forensics: no daemon ran in the stopped attempt

The retained supervisor calls host preflight before opening the cell log and
before `Popen`. Preflight raised at that call. There is no cell execution receipt,
daemon stderr, daemon exit code or measured workload for this attempt. It cannot
be a 257th-internal-defect shutdown of a daemon that had not started.

The host journal records an unauthenticated SSH session closing inside the
three-second preflight interval. That is a plausible explanation, **not an
identified match**: the old receipt discarded the differing PID/start-time pair.
The controller's own SSH session closes after the failed preflight. Raw journal
and process evidence are retained privately; no addresses or account names are
published. The probe now retains up to 32 PID/start-time/name rows per endpoint,
with unavailable identities and omitted rows explicit. The strict receipt reader
accepts historical receipts without the supplement and checks its shape, bounds
and agreement with churn totals when present. Its full-inventory fail-closed
verdict is unchanged; arguments, environment and paths are excluded.

The earlier failed third current-baseline attempt was different. Its wrapper
exited 1 after clean seed shutdown and a read-only pre-measurement inventory open
failure, before spawning the load daemon. Its log has no runtime-stop reason or
visible internal-defect burst. Shutdown release census lines occur in successful
cells too and do not themselves prove the breaker fired. The underlying inventory
open error was collapsed to a stage label, so its cause remains unknown; absence
of logged defects is not proof that the private incident ring was empty.

The active repair branch already descends from local main `68f696b50`, including
ADR-0250's scoped, rate-based breaker. No obsolete `InternalError::new` conversion
or rebase is needed here. The historical benchmark binaries are a different
source lineage and lack that fix. No remote benchmark was retried during this
forensic follow-up.

### Retain counts during the workload

With `RIFFDB_WRITER_BATCH_DIAGNOSTICS=1`, the ready-process supervisor emits
`riffdb-checkpoint-progress-v1` to stderr immediately and then once per second,
up to 600 snapshots per ready lifecycle. Each row carries PID, sample ordinal,
limit, monotonic elapsed milliseconds and the 15 checkpoint-census counters.
Missed timer ticks are skipped, not replayed in a burst. The ordinary final
shutdown census is unchanged. The diagnostic runs outside the writer; it has
no established timing-overhead verdict.

The harness must open and retain the daemon log before launch, and must preserve
it on failure even without a final JSON report. These are cumulative live
snapshots, not an event log: a crash can lose the interval since the last row,
supervisor starvation can delay it, and the sample cap ends live coverage. Atomic
fields can straddle an update. Separate ready lifecycles and never sum successive
snapshots. A counter-only local exercise checks collection before any further
remote timing work; its timings are not performance evidence.

### Local counter result: the byte limit fires before 4,096 transitions

One bounded local exercise used the current-main-based repair source, Rust
1.98.1 debug binaries, the smoke write-only workload with 32 clients, a 90-second
load and no warmup. This is **counter evidence only**, not a benchmark comparison.
All 40,730 application commands succeeded; none were unavailable or errored.

| Recorded load-generation counter | Value |
| --- | ---: |
| Checkpoints selected / materialized / installed | 12 / 12 / 12 |
| Byte-triggered / transition-triggered selections | 12 / 0 |
| Physical-limit / reanchor triggers; worker/spawn failures | 0 / 0; 0 |
| Selected transitions per checkpoint, minimum–maximum | 3,069–3,278 |
| Selected encoded bytes per checkpoint, minimum–maximum | 16,804,979–17,845,329 |
| Total selected transitions / commands | 37,936 / 37,936 |
| Total selected encoded bytes | 208,327,857 |
| Live load-generation snapshots retained | 91 |

The last live row was written before the final shutdown census and matches all
15 final counters. Positive selections were retained throughout the workload,
so collection no longer depends on a successful shutdown or final report.
Seed and load generations are kept separate. The remaining acknowledged commands
belong to the suffix, not additional asynchronous checkpoint selections.

This establishes **byte-limit selection below 4,096 transitions for this run**.
It does not establish historical parent checkpoint counts, the exact 3,046
prediction, a C3D result, or checkpoint causation of P99. The local debug run's
timings and stage ordering must not be used for latency attribution. Frozen
source diff, binary digests, the attempt and full logs are retained privately.
The load log SHA-256 is
`a2f5c61296ddaa5befae0140d072d79ffc306c1f6b03b1ec9ff700348f5087aa`;
the final report SHA-256 is
`7467027ab8345fb0ea1f8b583aeb1d6f7660d266aff54f99042c777672608c7a`.

## Final disposition

The [single correlated release run](wp749-correlated-tail-2026-09.md) completed on
2026-09-21 UTC. It names the checkpoint-conditioned pipeline drain and long
journal-sync wait path, preserves the unassigned request-time remainder, and
ends this tail investigation. The maintainer lifted the WP-748 pause. The
production-capture sync cost is tracked separately in
[issue #2](https://github.com/riffdb/riffdb/issues/2).

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
The earlier report correction also passes scoped acceptance with its own build
directory. A shared-directory attempt picked up mismatched dependency artifacts;
isolated compilation passed without a source change. The paused worktree was
removed after verifying a separate bundle containing its complete history; its
branch remains retained.

## Review note

- Package: WP-749 investigation; no completion claim.
- Tier: guarantee for the original repair; this follow-up is internal diagnostics
  within accepted semantics.
- Behavior: version complete receipt-wait diagnostics; retain live checkpoint
  counts and bounded changed process identities; distinguish old incomplete
  reports and capture cost from tail attribution.
- Checks: original receipt repair passed 1,539 tests and scoped acceptance.
  Follow-up: 469 server tests passed (eight existing profile skips), two explicit
  timer-schedule tests, host-probe and strict-reader self-tests, Clippy and
  formatting. The initial handbook check lacked its pinned tools on `PATH`;
  exposing the existing pinned tools makes it pass without a source workaround.
  The remote attempt failed preflight; the local counter collection succeeded.
- Handbook: this page, the writer-evidence defect warning and app-baseline schema
  guide.
- Compatibility: existing durable bytes, public API and checkpoint decisions
  unchanged; diagnostic readers must recognize the V2 publication line.
- Hazards: overlapping wait sums, independent atomic snapshots, W1/W2 wall-time
  defects, and aggregate-only checkpoint attribution remain explicit limitations.
