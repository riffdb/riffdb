---
adr: 0184
title: Panic Discipline, Test Economics, and Coordinator Simulation
status: accepted
tier: surface
date: "2026-09-01"
accepted: "2026-09-01"
requires: [ADR-0012, ADR-0058, ADR-0098, ADR-0101, ADR-0104, ADR-0113, ADR-0124]
amends:
  - SPEC 9.8 (fail-fast is made testable)
  - SPEC 17.10 (adds the ADR-0113 Phase-2 coordinator harness)
  - SPEC 18.2 (the unit and integration job becomes partitioned and time-recorded)
  - ADR-0113's "future ADR" for Phase 2
# ADR-0179 and ADR-0180 are not amended; this record only adds a size guard their
# packages run.
requirements: [TEST-002, TEST-003, TEST-004, SIM-008, SIM-009]
# Builds on TEST-001, SIM-001 through SIM-007, SYS-003, and PERF-007.
packages: [WP-764, WP-765, WP-766, WP-767]
obligations:
  - id: OBL-0184-1
    package: WP-764
    proof: check-panic-allowances
    says: The lib targets of riffdb-commit, riffdb-storage-api, riffdb-storage-redb,
      riffdb-service, and riffdb-server compile with unwrap_used, expect_used,
      panic, and unreachable denied, and every surviving allowance is an
      "#[expect]" with a reason naming the invariant.
  - id: OBL-0184-2
    package: WP-764
    proof: writer_thread_panic_stops_the_coordinator_and_exits_the_daemon_nonzero
    says: A panic on the riffdb-command-writer thread stops the coordinator,
      refuses later admission with a typed outcome, and makes riffdbd exit nonzero
      instead of serving.
  - id: OBL-0184-3
    package: WP-764
    proof: generators_use_the_infallible_writer_and_carry_no_fmt_expect
    says: The four language generators write through one infallible string-write
      helper and carry no expect on fmt::Write.
  - id: OBL-0184-4
    package: WP-765
    proof: check-test-partition-coverage
    says: The CI test job runs under cargo-nextest in fixed partitions, every
      "[[test]]" target of every workspace crate is scheduled exactly once across
      the partitions, and the recorded wall time is written to the job summary and
      to the CI wall-time evidence file under release/evidence.
  - id: OBL-0184-5
    package: WP-766
    proof: coordinator_interleaving_decisions_feed_the_digest
    says: The coordinator harness reproduces a byte-identical trace digest for one
      seed and a different digest when any interleaving or crash decision changes.
  - id: OBL-0184-6
    package: WP-766
    proof: every_coordinator_crash_window_recovers_to_the_model
    says: Every seeded coordinator crash window recovers to the testkit model at
      the recovered frontier, including windows between epoch seal, durable fence,
      and publication.
  - id: OBL-0184-7
    package: WP-767
    proof: reactive_module_artifacts_are_byte_identical_after_rename
    says: riffdb-reactive-syntax replaces riffdb-query-syntax with byte-identical
      reactive-module artifacts and no production crate depends on riffdb-sim.
  - id: OBL-0184-8
    package: WP-767
    proof: check-file-size-guard
    says: No source file may grow past 10,000 lines, and growth past 6,000 lines is
      reported, in every package that touches one of the five largest files.
review_triggers:
  - A panic-capable call would be kept in a durable crate without a reason naming
    the invariant, or a crate-level blanket allowance would be introduced.
  - The daemon would continue acknowledging commands or reporting readiness after
    the writer panic flag is set, or the fail-fast test would be weakened to a log
    assertion.
  - A production crate would gain a dependency on riffdb-sim, a test clock, or a
    seed source.
  - A rename, lint, or CI change would alter any durable byte, artifact hash, or
    public protocol.
---
# ADR-0184: Panic Discipline, Test Economics, and Coordinator Simulation

## Context

SPEC 9.8 says the commit coordinator "should fail fast on an invariant breach
indicating internal corruption rather than continue serving uncertain state",
and that runtime and compiler entry points do not panic on user input. Neither
sentence is machine-checked. The workspace lints in `Cargo.toml` forbid unsafe
code and deny unused results but set no level for `clippy::unwrap_used`,
`clippy::expect_used`, `clippy::panic`, or `clippy::unreachable`. Running those
four lints as warnings over the library targets at commit `0c41b7bb` reports
38 sites in `riffdb-commit`, 35 in `riffdb-storage-api`, 31 in
`riffdb-storage-redb`, 24 in `riffdb-service`, 39 in `riffdb-server`, and 552
in `riffdb-query-module`. The last figure is one pattern: the Rust, Go,
Python, and TypeScript generators call `.expect` on `fmt::Write` into a
`String`, which cannot fail. The durable-crate sites are a mix of genuine
invariants ("group cap fits u16", "digest key ID") and lock, clock, and
channel calls whose failure is a defect the process should not survive.

The writer already carries a `writer_panicked` flag and the publication path
wraps its closure in `catch_unwind`
(`crates/riffdb-commit/src/audit_executor.rs`). What happens after the flag
is set is not pinned by a named test: `RunningCommandCoordinator::shutdown`
maps it to `CoordinatorShutdownError::ActorPanicked`, but no test drives a
panic on the `riffdb-command-writer` thread through `riffdbd` and asserts the
daemon's exit code. A fail-fast policy that is not tested is a policy that a
later refactor can silently turn into a hung or degraded process.

The test estate is large and unmeasured. The workspace holds 4,667 test
functions and roughly 395,000 lines of test code, 47 percent of all Rust in
the repository, across 238 integration test targets, 30 of which are root
`tests/` binaries owned by `riffdb-testkit` and five by `riffdb-server`. The
required CI job in SPEC 18.2 is one `cargo test --workspace --all-features`
invocation on one runner (`.github/workflows/ci.yml`). Its wall time is
recorded nowhere in the repository, so no package can show that it made the
suite slower or faster, and the single job serializes link and run time that
partitions would overlap.

ADR-0113 committed Phase 1 of deterministic simulation: `riffdb-sim` drives
the real redb backend and journal media over a seeded `SimDisk`, checks
recovery against the testkit model, and retains a seeded campaign corpus.
Phase 2, "writer-pipeline interleaving", was named as a future record and
left blocked on a monotonic-time seam for the coalescing decisions in
`audit_executor.rs`. The coordinator, writer, journal, and changelog lanes
therefore still rely on hand-written crash arms and on the process-level
matrix for their concurrency evidence. `riffdb-conflict` already exposes a
`DeterministicConflictScheduler` and `ConflictSchedulePoint` behind its
`loom` and `shuttle` features, so one deterministic step scheduler exists in
the tree today; it is not composed with the disk simulator.

Two smaller facts belong in the same record because they are hygiene the
larger consolidation packages will otherwise pay for repeatedly.
`crates/riffdb-query-syntax` is the reactive-module grammar (`parse_module`,
`REACTIVE_GRAMMAR_VERSION_V1`) and sits beside `riffdb-riffql-syntax`, the
actual query grammar. Five source files exceed 10,000 lines
(`riffdb-cli/src/app.rs` 14,583; `riffdb-storage-redb/src/startup.rs`
13,453; `riffdb-service/src/dto.rs` 11,622; `riffdb-proto/src/public_message.rs`
11,549; `riffdb-contract-ir/src/plan.rs` 10,447) and
`riffdb-storage-redb/src/store.rs` is 10,366. ADR-0179 and ADR-0180 remove
the reason most of them exist; this record only stops them growing further
while that work lands.

## Decision

### 1. Deny panic-capable calls in the durable crates

`riffdb-commit`, `riffdb-storage-api`, `riffdb-storage-redb`,
`riffdb-service`, and `riffdb-server` set `clippy::unwrap_used`,
`clippy::expect_used`, `clippy::panic`, and `clippy::unreachable` to `deny`
for their library targets. Test targets, examples, and benches are exempt.
Every remaining site is either converted to a typed error or kept behind
`#[expect(clippy::<lint>, reason = "...")]` whose reason names the invariant
that makes the failure impossible or the reason the process is required to
stop. A blanket crate-level `#![allow]` is not permitted. A script,
`scripts/check-panic-allowances`, lists every allowance with its reason and
fails on any allowance without one, so the residue is an enumerable ledger
rather than prose.

The lint level is a workspace fact registered as a `VER-*`-independent
repository policy; it changes no durable format, protocol, or IR.

### 2. Make fail-fast a tested property

SPEC 9.8's fail-fast sentence becomes `TEST-002`: a panic on the
`riffdb-command-writer` thread stops the coordinator, later admissions
receive the existing typed unavailable outcome rather than hanging, and the
daemon process exits nonzero within its configured drain deadline. The proof
drives a test-only failpoint on the writer thread through `riffdbd` and
asserts the exit status and the absence of any acknowledgement after the
panic. Whether the process aborts immediately or drains readers first is an
implementation choice inside the deadline; what is fixed is that no command
is acknowledged and no readiness is reported after the flag is set.

Rust's default `panic = "unwind"` is retained so the existing `catch_unwind`
at publication keeps releasing capabilities per SPEC 9.8; the daemon's
nonzero exit is produced by the lifecycle, not by `panic = "abort"`.

### 3. Give the generators one infallible writer

`riffdb-query-module`'s Rust, Go, Python, and TypeScript generators write
through one helper that appends to a `String` and cannot fail, replacing the
552 `expect` calls on `fmt::Write`. Generated artifacts are byte-identical
before and after, which the existing generated-fixture check proves. This
decision is deliberately narrow: it does not change how templates are
authored, which ADR-0179 decides.

### 4. Partition the required test job under cargo-nextest

SPEC 18.2's "Unit and integration" job runs under cargo-nextest with exactly
four fixed whole-target partitions, one job per partition, on the pinned
toolchain. A test target's canonical nextest binary ID is assigned by the
first eight bytes of its SHA-256 digest modulo four; each job passes the
resulting exact binary-ID filterset to nextest. No target is split across jobs.
The root `[[test]]` targets remain
explicit targets in their owning crate manifests, unchanged, and are
scheduled by nextest like every other target; `scripts/check-test-partition-coverage`
enumerates every test target from `cargo metadata` and proves each is
scheduled exactly once across the four partitions, so no target is silently
skipped by partitioning. `cargo test --workspace --all-features` remains a
valid local command and remains in `scripts/ci-all`.

Each partition writes its measured wall time to the job summary. A
release-evidence file, `release/evidence/ci-wall-time-v1.json`, records the
pre-partition single-job time measured once before WP-765 lands, the
per-partition times after, and a target of at most 20 minutes for the
longest partition. The file is evidence, not a gate: a package that exceeds
the target reports it; it does not fail CI on a slow runner.

### 5. Extend deterministic simulation to the coordinator

`riffdb-sim` gains an ADR-0113 Phase-2 harness composing `SimDisk`, the
`riffdb-conflict` deterministic scheduler, and a seeded step scheduler over
the coordinator's lanes. The scheduler chooses, from the seed alone, the
interleaving of admission, preparation, epoch seal, durable fence,
publication, completion, and changelog emission across a bounded set of
concurrent commands, and may place a crash at any of those points, including
between a sealed epoch and its fence and between a fence and its publication.
Every scheduling and crash decision feeds the versioned trace digest
(`SIM-008`). After every simulated crash the reopened engine passes startup
validation and equals the testkit `AuthoritativeCommandModel` at the
recovered frontier (`SIM-009`), extending the model where its coverage is
narrower.

The production footprint is the monotonic-time seam ADR-0113 named:
the coalescing decisions in `audit_executor.rs` read time through a port the
harness can drive, with the production port unchanged in behavior. No
production crate depends on `riffdb-sim` (`SIM-007`), enforced by the
existing metadata-driven architecture test. Loom and Shuttle remain available
for lane-level primitives under SPEC 17.4; the harness does not replace them.

### 6. Rename the reactive grammar crate

`riffdb-query-syntax` becomes `riffdb-reactive-syntax`. The rename changes
the package name, directory, and imports in `riffdb-query-compiler` and
`riffdb-query-module`; it changes no grammar, artifact bytes, module hash, or
version constant. `REACTIVE_GRAMMAR_VERSION_V1` and every reactive-module
fixture remain byte-identical.

### 7. Guard file size during consolidation

`scripts/check-file-size-guard` reports every non-generated Rust source file
above 6,000 lines and fails when a file above 10,000 lines has grown relative
to the merge base. Files already above 10,000 lines may shrink or stay equal;
they may not grow. The guard runs in the acceptance commands of every package
under ADR-0179 and ADR-0180 that touches one of the six files named in
Context, and in `scripts/ci-all`. It does not run on generated sources.

### 8. Explicitly deferred

A network metrics exporter, distributed tracing, and any change to the
in-process metrics model remain out of scope. ADR-0113 Phase 3, whole-daemon
simulation, remains deferred. Splitting the six large files is owned by
ADR-0179 and ADR-0180; this record only prevents growth.

## Options Considered

1. **Set `panic = "abort"` in the release profile and stop there.** Rejected:
   it removes the `catch_unwind` that SPEC 9.8 relies on to release
   capabilities on a containable evaluation panic, and it proves nothing
   about which panics exist. The lint ledger plus a named fail-fast test
   pins both halves.
2. **Warn rather than deny the panic lints workspace-wide.** Rejected: 1,000
   warnings are noise nobody reads, and the crates where a panic is a
   product failure are exactly five. Denying there and leaving the rest at
   the default keeps the signal where the risk is.
3. **Shard CI by crate list instead of nextest partitions.** Rejected: a
   hand-maintained crate list drifts as crates are added, and the review
   found the crate graph itself is about to change under ADR-0180. Hash
   partitioning over discovered targets plus a coverage script needs no
   maintenance and cannot skip a target.
4. **Build the coordinator harness on Shuttle alone.** Rejected for the same
   reason ADR-0113 gave for Phase 1: Shuttle explores thread interleavings
   but does not own the disk, so a crash between seal and publication cannot
   be torn and recovered in the same run. Composing the existing sim disk
   with a seeded step scheduler covers both; Shuttle stays for primitives.
5. **Leave `riffdb-query-syntax` named as is.** Rejected: the cost is one
   rename with no format change, and the name misleads every reader,
   including the agents ADR-0056 says are the primary builders.

## Consequences

- Panics in the durable crates become an enumerated, reasoned ledger, and
  fail-fast is a tested daemon property rather than a sentence in SPEC 9.8.
- Test wall time becomes a recorded quantity with a target, so every later
  package can state its effect on the suite.
- The coordinator's concurrency evidence gains seeded exploration and a
  retained corpus, the same shape Phase 1 gave the storage engine.
- Cost: converting roughly 170 durable-crate sites to typed errors or
  reasoned allowances; four CI jobs instead of one, with four target builds
  unless a shared cache is used; one monotonic-time seam in the writer.
- Cost: the rename touches every import in two crates and the handbook's
  crate inventory in one commit.
- Deferred: metrics export, distributed tracing, Phase-3 simulation, and the
  decomposition of the six large files.

## Compatibility

No public API, durable record, storage key, journal frame, contract or
query IR, source language, generated artifact, or migration changes.
`riffdb-reactive-syntax` is a workspace-internal rename; the reactive
grammar version and every fixture byte are unchanged, which WP-767 proves by
regenerating and diffing the reactive-module fixtures. The version topology
registers no new domain. CI configuration and workspace lint levels are
repository policy, not release-significant identities.

## Security

Denying panic-capable calls in the crates that hold authoritative state and
serve authenticated requests removes a class of denial-of-service where
malformed but authenticated input reaches an `expect`. The fail-fast proof
ensures a panicked writer cannot keep acknowledging or reporting readiness.
The simulation harness and the monotonic-time port are test seams; the
production port is the existing wall clock and the harness cannot be
selected by configuration, a request, or an environment variable in a
release binary. The file-size guard, partition-coverage script, and panic
ledger read repository files only and make no network calls.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** No public surface is added
  or changed. An application or agent cannot select a lint level, a CI
  partition, a simulation seed, or a time port through any transport,
  contract, RiffQL, generated binding, CLI, or MCP surface. The writer
  failpoint that drives the fail-fast proof exists only under a test
  feature and is absent from release binaries.
- **Scale:** The coordinator harness bounds its concurrent command set and
  its crash schedule and does not require the whole database in memory; it
  inherits Phase 1's bounded-disk model. Nothing here assumes co-located
  storage beyond what ADR-0113 already assumes for single-node simulation,
  and the follower topology named in ADR-0093 remains a later simulation
  target as ADR-0113 already records.

## Testing

- `scripts/check-panic-allowances` (OBL-0184-1) fails on any denied lint
  reaching a library target without a reasoned `#[expect]`, and prints the
  ledger.
- `writer_thread_panic_stops_the_coordinator_and_exits_the_daemon_nonzero`
  (OBL-0184-2), a root integration test owned by `riffdb-server`, arms a
  test-only failpoint on the writer thread, submits a command, and asserts
  no acknowledgement, a typed unavailable outcome for a later submission,
  and a nonzero process exit within the drain deadline.
- `generators_use_the_infallible_writer_and_carry_no_fmt_expect`
  (OBL-0184-3), an architecture test in `riffdb-query-module`, plus the
  existing generated-fixture byte check.
- `scripts/check-test-partition-coverage` (OBL-0184-4) enumerates test
  targets from `cargo metadata`, runs nextest's list mode per partition, and
  asserts exactly-once scheduling.
- `coordinator_interleaving_decisions_feed_the_digest` (OBL-0184-5) and
  `every_coordinator_crash_window_recovers_to_the_model` (OBL-0184-6) in
  `riffdb-sim`, alongside the retained Phase-2 corpus replayed per merge
  under `SIM-004`.
- `reactive_module_artifacts_are_byte_identical_after_rename`
  (OBL-0184-7) and the existing `no_production_crate_depends_on_the_simulator`
  architecture test.
- `scripts/check-file-size-guard` (OBL-0184-8) with a self-test that
  fabricates a grown file above the ceiling and proves refusal.

## Requirements and Work Packages

- **Requirements:** `TEST-002` through `TEST-004` and `SIM-008` through
  `SIM-009`; builds on `TEST-001`, `SIM-001` through `SIM-007`, `SYS-003`,
  and `PERF-007`
- **Defines or blocks:** WP-764 through WP-767
- **Final evidence:** WP-766

## Decision Deadline

Exact acceptance is required before WP-764 changes a workspace or crate lint
level, before WP-765 replaces the SPEC 18.2 test job, and before WP-766 adds
the monotonic-time seam to `riffdb-commit`. WP-767's rename and size guard
may proceed on direction approval because they change no behavior, but the
record as a whole is accepted or rejected as one text.

## Acceptance

Direction approved 2026-09-01; exact text accepted 2026-09-01. The maintainer
accepted the exact text of this record in the Claude Code session of
2026-09-01, all seven consolidation records together.
