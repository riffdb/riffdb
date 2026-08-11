# ADR-0113: Deterministic Simulation Testing for the Durable Engine

- **Status:** Accepted
- **Direction approved:** 2026-08-09
- **Exact text accepted:** Yes — 2026-08-09, maintainer acceptance as written
- **Amended:** 2026-08-11 — maintainer-directed repair after the
  delete-aware entity-chain layout change moved the pinned physical crash
  window; receipted witness rotation added without relaxing active defect
  reproduction
- **Decision deadline:** Satisfied — accepted before any simulation work
  package merged production-code seams

## Context

Trust in a database is actuarial: operators believe storage engines that have
survived years of production failure, and RiffDB is months old. The existing
crash evidence is strong but hand-authored and linear in effort: every
`storage_recovery_matrix` arm is a hand-written scenario costing a full
fork+exec+SIGABRT cycle, armed through a one-shot `RedbTestController` that
fires a single operation per process lifetime
(`crates/riffdb-storage-redb/src/hooks.rs:328-345`), and the testkit's own
failpoint inventory names its uncovered seams `ProductionGap`
(`crates/riffdb-testkit/src/failpoint.rs`). Three recent durable-path defects
— the unbounded shutdown-checkpoint history walk, the migration preflight that
scanned zero rows against a journal-authoritative overlay, and the allocator
inspection whose disjointness assumption held only by accident — were each
caught by exactly one detector, late. Scenario enumeration by hand does not
explore; it confirms.

Deterministic simulation testing (DST) is the established answer: run the real
engine against a simulated disk and simulated time under a seeded fault
schedule, so that thousands of process lifetimes execute per CPU-hour and
every failure ever found replays exactly from a seed. FoundationDB and
TigerBeetle demonstrated that a database designed for simulation can compress
years of production exposure into continuous CI.

RiffDB is unusually cheap to simulate because determinism is already contract,
not aspiration:

- The command runtime performs no clock, network, filesystem, or entropy
  access (`AGENTS.md` boundary 4), enforced by source-text architecture tests
  (`crates/riffdb-runtime/tests/architecture.rs:20-100`), and time enters as a
  value (`TransactionContext::new(…, tx_time: LogicalTime, …)`).
- The service layer's clocks, deadline scheduler, job spawner, cursor entropy,
  and identifier sources are already `Arc<dyn>` ports injected through the
  process-graph builder (`crates/riffdb-server/src/clocks.rs`,
  `crates/riffdb-server/src/process_graph.rs:498-537`); the only entropy
  dependency in the production graph is a checksum-pinned `getrandom` behind
  trait seams.
- A single writer holds all mutation authority (`SPEC.md` PERF-007), so
  storage-level histories are serial by construction.
- An independent reference model of authoritative state already exists —
  `AuthoritativeCommandModel` with eleven typed defect classes
  (`crates/riffdb-testkit/src/model/authoritative.rs`) — but is wired only to
  the memory backend, never to the redb engine that ships.
- redb 4.1.0 publicly exposes `StorageBackend` and
  `Builder::create_with_backend`, and redb's own test suite drives a
  fault-injecting backend through that seam — the exact pattern this ADR
  adopts, already proven against the pinned engine version.

Two gaps block simulation today. First, `RedbStore::open` hands redb a raw
path (`crates/riffdb-storage-redb/src/store.rs:946-954`), and the journal lane
moves a raw `std::fs::File` into its worker thread with an uninterceptable
`sync_data()` (`crates/riffdb-storage-redb/src/journal.rs:1523-1543`, `:1673`)
— the newest, most complex durable code has the least injectable I/O. Second,
a bounded inventory of ambient nondeterminism sits above storage: bare
`Instant::now()` reads feeding batch-formation coalescing decisions
(`crates/riffdb-commit/src/audit_executor.rs`), a worker count taken from
`available_parallelism()`, and `HashSet` (random iteration order) in the
conflict path of an otherwise BTree-only codebase
(`crates/riffdb-commit/src/command_execution.rs:293-295`, `:1229-1481`).

Standing obligations this decision discharges rather than invents: ADR-0095
gates the serial micro-batch path on a "deterministic schedule equivalence"
proof that has no harness today (`adr/0095:196-210`); `AGENTS.md:143` requires
an explored or deterministic schedule test for every concurrency primitive;
ADR-0109 requires deterministic schedules for lease races; and the alpha
freeze names "deterministic race/crash schedules" as required evidence.
ADR-0012 simultaneously constrains the design: no test clock, scheduler seed,
or fuzz seed may compile into the production runtime path
(`adr/0012:137-139`), so the simulator must be a separate composition root
over production components, never a runtime mode of the daemon.

## Proposed Decision

Adopt deterministic simulation testing in three phases. This ADR commits
Phase 1 and names Phases 2 and 3 as explicit future decisions.

**Phase 1 — storage-engine simulation (committed by this ADR).**

1. **Simulated disk.** A `SimDisk` maintaining, per simulated file, a durable
   image (contents as of the last acknowledged sync) and a volatile image
   (writes not yet synced), with a seeded fault schedule drawn from a
   deterministic PRNG: crash points at any write or sync boundary, torn writes
   at configurable granularity applied to unsynced regions on crash, transient
   I/O errors, and space exhaustion. Two adapters expose it: an implementation
   of redb's `StorageBackend` for the engine file, and an implementation of a
   new internal **journal media port** for the journal, checkpoint, spare, and
   marker side files.
2. **Seams.** `RedbStore` gains a backend-parameterized open following the
   existing hidden test-surface convention (`open_with_test_controller`,
   `#[doc(hidden)]`); the journal lane's worker is parameterized over the
   journal media port (`write_all_at`, `read_exact_at`, `sync_data`,
   `set_len`, rename/remove for extent recycling), with the production
   implementation remaining `std::fs::File` and the production call sites
   unchanged in behavior. Maintenance, backup, retention, and columnar file
   I/O stay on the real filesystem in Phase 1; they already carry file-level
   failpoints and are out of the seeded exploration's scope.
3. **In-process crash-recovery loop.** A simulated crash discards volatile
   images, drops every engine handle, and reopens the store from the durable
   images in the same process — replacing fork+exec+SIGABRT per scenario with
   thousands of crash-recovery cycles per CPU-minute, driven to arbitrary
   depth (crash during recovery, crash during the recovery of a recovery).
4. **Reference-model oracle.** `AuthoritativeCommandModel` is wired to the
   simulated redb store: the workload driver applies every acknowledged
   command to the model, and after each simulated recovery the recovered
   engine state must equal the model's state at the recovered durable
   frontier, extending the model where its coverage is narrower than the
   engine's. Startup validation and the structural inspection pass run on
   every recovery as today.
5. **Seeded workload generator.** Command mix, contention profile, batch
   shapes, and migration interleavings generated from the seed over compiled
   fixture contracts, with identities derived deterministically
   (`from_unix_milliseconds_and_random` from seed material) — generalizing the
   crash matrix's hand-fixed fixtures into an explorable space.
6. **Determinism proof and corpus.** Same seed and generator version produce a
   byte-identical execution trace hash, pinned by a test that runs the same
   seed twice. A failure minimizes to a seed plus schedule prefix; every bug
   the simulator finds lands as a pinned seed in a regression corpus replayed
   per-merge, with open-ended exploration running on a nightly budget. A
   physical crash-placement witness may move when an intentional durable-layout
   change adds or removes engine writes. Such a finding requires a receipted
   witness rotation: retain and continue replaying the historical coordinate,
   annotate it with the exact causal commit and review date, append a stable
   successor that reproduces the same exact expected territory under the
   current layout, and keep at least one active engine-defect witness until the
   engine pin contains the fix. A later layout change extends the forward-only
   successor chain rather than rewriting an earlier receipt. Silently deleting
   the old seed, accepting a green replay without a successor, or weakening an
   expectation remains forbidden.
7. **Corpus subsumption.** Every existing `storage_recovery_matrix` arm is
   expressible as a DST schedule; the matrix remains as process-level
   evidence, and the simulator owns exploration.

**Enabling hygiene (part of Phase 1, each justified independently of DST):**
replace `HashSet` with `BTreeSet`/`BTreeMap` in the conflict-detection path,
freezing the codebase's BTree-only convention with an architecture test; make
the evaluation-pool worker count an explicit input with
`available_parallelism()` as the production default.

**Phase 2 — writer-pipeline interleaving (future ADR).** Seed-controlled step
scheduling across the coordinator, writer, journal, and changelog lanes to
explore the ADR-0098/0101 pipeline's concurrency, discharging ADR-0095's
serial-equivalence gate. Blocked on giving the coalescing decisions in
`audit_executor.rs` a monotonic-time seam; Loom/Shuttle (already sanctioned
vocabulary in `AGENTS.md`) are candidate mechanisms to evaluate there.

**Phase 3 — whole-daemon simulation (explicitly deferred).** In-process
transport (tonic accepts custom incoming streams) plus simulated task
scheduling. Deferred until Phases 1–2 leave a demonstrated uncovered bug
class; possibly never needed.

**Composition.** The simulator lives in a new dev-only crate (working name
`riffdb-sim`) that composes production components with simulated ports. No
production crate depends on it, enforced by an architecture test, satisfying
ADR-0012's prohibition on test clocks and seeds in the production path. The
production-code footprint of Phase 1 is exactly: the backend-parameterized
open, the journal media port, and the two hygiene items.

## Options Considered

1. **Adopt an external simulation framework (madsim, turmoil, shuttle, loom).**
   Rejected for Phase 1: those tools target async-network topologies or
   lock-level interleavings, while the Phase-1 bug class is durable-state
   correctness below redb, where the storage-backend seam gives exact,
   dependency-free control. Loom/Shuttle remain candidates for Phase 2's lane
   scheduling.
2. **Extend the hand-written crash matrix instead.** Rejected: arms scale
   linearly with authoring effort, cost a process spawn each, cannot compose
   faults (crash-during-recovery), and confirm anticipated failures rather
   than exploring unanticipated ones.
3. **Whole-daemon simulation first.** Rejected: making the multi-thread tokio
   runtime, port workers, and real TCP deterministic is the largest possible
   first bite, and the highest-value bug class (durable-state loss) lives
   below the service layer, reachable by the smallest one.
4. **External chaos/DST service (Antithesis-style).** Complementary later, but
   the survival argument requires the evidence to be public, free, and
   replayable by anyone from a seed — repo-native simulation is the primary
   instrument.

## Consequences

- Concurrency and durability claims gain a falsifiable, replayable form: a
  16-byte seed reproduces any found failure exactly, and the corpus becomes
  public evidence of survived fault-years.
- The journal lane — the newest and most complex durable code — becomes the
  first exploration campaign target, which is also why its seam change
  deserves the most careful review.
- CI cost is bounded and split: corpus replay per-merge (minutes), seeded
  exploration nightly (budgeted hours).
- The reference model gains its first wiring against the shipping engine,
  closing the gap where only the memory backend was model-checked.
- Phases 2 and 3 are explicitly deferred; Phase 1 buys no interleaving
  coverage above the storage layer.
- The one-shot `RedbTestController` remains for targeted process-abort
  evidence; the simulator does not replace it.

## Compatibility

No public API, wire-format, or durable-format change. The backend-parameterized
open and the journal media port are internal seams following the existing
`#[doc(hidden)]` test-surface convention; production defaults preserve current
behavior byte-for-byte, including journal file layout. The simulator crate is
never a dependency of any shipped binary.

## Security

No new trust boundaries: the simulator is a development harness operating on
synthetic workloads below the authorization layer. Fault schedules cannot be
expressed through any public surface. Simulation logs contain only synthetic
fixture data, so no redaction surface is added.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** no application-facing surface
  is added or changed; the new constructors are hidden test surfaces
  unreachable through client, CLI, MCP, or SDK paths, and the architecture
  test forbidding `riffdb-sim` in production dependency graphs freezes the
  boundary.
- **Scale:** the simulator assumes a single node because a single node is the
  current deployment unit, not because the design requires it; ADR-0093's
  follower topology is a named Phase-2/3 simulation target, and the sim-disk
  state is bounded by the configured workload, not by database size
  assumptions.

## Testing

- Determinism pin: one test runs the same seed twice and asserts identical
  trace hashes; the trace-hash format is versioned with the generator.
- Backend conformance: `SimDisk`'s redb adapter passes a behavior suite for
  the `StorageBackend` contract (length, zero-initialized growth, read-back,
  sync semantics); the journal media port carries one shared behavior suite
  run against both the production `File` implementation and the simulated one.
- Oracle wiring: model-equality assertion after every simulated recovery, plus
  the existing startup validation and structural inspection passes.
- Corpus: every `RECOVERY_SCENARIOS` row tagged for the storage layer is
  reproduced as a pinned schedule; found-bug seeds accumulate as regression
  fixtures. A physical-layout witness rotation proves both sides: the
  historical coordinate no longer reaches its exact old territory for the
  named layout commit and a later successor chain terminates in an active
  witness that still reproduces the pinned engine defect. (As
  delivered by WP-583: the engine-commit crash arms carry
  pinned campaign schedules, while the migration-batch arms, the
  owner-typestate rows without a process crash point, and — until the engine
  pin advances past upstream `fd82ced` — the redb 4.1.0 file-growth crash
  placement are TYPED, guard-enumerated exclusions in the classification
  machinery rather than pinned schedules.)
- Architecture tests: `riffdb-sim` absent from all production dependency
  graphs; conflict-path BTree-only pin; existing runtime determinism checks
  unchanged.

## Requirements and Work Packages

- **Requirements:** to be registered as a `SIM-*` family in `SPEC.md` at
  package time (determinism proof, fault-schedule coverage, model equality,
  corpus replay); the packages also carry existing `REC-*` recovery
  obligations forward.
- **Defines or blocks:** the Phase-1 simulation work packages (seams, SimDisk,
  oracle wiring, generator, corpus), numbered at package time — with the
  duplicate-id check against concurrent sessions that the merge ritual now
  requires.
- **Phase-1 foundation package:** WP-580 (`riffdb-sim` crate, SimDisk, redb
  backend adapter, determinism pin, and the two enabling hygiene items)
  registered the `SIM-*` family as SPEC §17.10; the remaining Phase-1
  packages (journal media seam, oracle wiring, generator, corpus) are
  numbered as they mint.
- **Oracle-wiring package:** WP-582 (testkit durable-inspection accessor
  extension, model comparison support, and the `riffdb-sim` recovery-oracle
  harness) delivers Phase 1 item 4's `SIM-003` evidence — model equality at
  the recovered durable frontier after every simulated recovery, with the
  startup-validation and structural-inspection passes as its precondition.
- **Seeded-campaign package:** WP-583 (seeded workload generator, the
  crash-schedule campaign driving the simulated store with the model in
  lockstep, the found-seed regression corpus, and the crash-matrix
  subsumption classification) delivers Phase 1 items 5-7's `SIM-004` and
  `SIM-006` evidence. Its first exploration surfaced a real redb 4.1.0
  crash-recovery defect (file growth left non-durable until the commit's
  single fsync, wedging the database unopenable after a torn crash; fixed
  upstream in `fd82ced`, unreleased) — retained as the corpus's inaugural
  entry behind a typed, pin-guarded placement exclusion until the engine pin
  advances.
- **Final evidence:** the Phase-1 exploration campaign report over the
  journal/extent/fold machinery.

## Decision Deadline

Exact acceptance is required before the first work package merges the
production-code seams (journal media port, backend-parameterized open,
conflict-path collection hygiene). Simulator-crate scaffolding that touches no
production crate may proceed under direction approval.
