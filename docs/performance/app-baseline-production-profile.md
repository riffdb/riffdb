# App-baseline production profile

The `--production` dataset tier and the offered-load rate sweep. Neither is
release evidence, and neither replaces the frozen `PERF-018` comparator.

## Why the tier exists

`full` seeds roughly 14,600 rows: 10 organizations, 500 users, 100 projects,
2,000 tickets, 8,000 comments, with `payload_bytes: 0`, so comment bodies are
short generated labels like `comment body 3/2/17/1`.

At that size every B-tree is shallow, the whole set is resident, and there is
almost no storage work for either engine to do. The profile therefore measures
protocol and client CPU cost rather than a database. That is consistent with
what the driver investigations found: every bottleneck located in the language
bindings this week — a per-value annotation resolution in Python, a
character-at-a-time parser and an O(n^2) number scan in TypeScript, a
marshal-then-unmarshal round trip per field in Go — and none in storage. Part of
that is because at 2,000 tickets there was no storage work available to be the
bottleneck.

The tier also under-measures the write path RiffDB is judged on, because
`payload_bytes: 0` means the durable frame carries almost no application bytes,
and WP-620 measured cloud fence latency rising sharply with frame size.

## What the tier is

| Knob | `full` | `production` |
| --- | ---: | ---: |
| organizations | 10 | 200 |
| users per org | 50 | 40 |
| projects per org | 10 | 12 |
| members per project | 5 | 6 |
| tickets per project | 20 | 50 |
| comments per ticket | 4 | 5 |
| labels per org | 5 | 12 |
| labels per ticket | 2 | 3 |
| comment body bytes | 0 | 256 |
| tickets | ~2,000 | ~120,550 |
| comments | ~8,000 | ~602,750 |
| approximate rows | ~14,600 | ~1,112,350 |

Board density is deliberately unchanged: `board_page_450` reads one bounded
page, and the point of this tier is the size of the data around that page.

Generation cost measured on the workstation: 374 ms, 489 MB peak RSS for the
in-memory dataset.

## Running it

```bash
benchmarks/run-app-baseline-typescript --production --backend both \
  --load-concurrency-sweep --reps 3

benchmarks/run-app-baseline-rate-sweep --language typescript --scale production \
  --rates "500 2000 5000 10000 20000"
```

`--production` is accepted by every language runner. Reports carry
`scale: "production"`, so a production cell cannot be mistaken for a `full` one.

## The rate sweep

The closed-loop sweeps answer "what does this saturate at". With zero think
time they cannot answer "what latency does a caller see at N operations per
second", because offered load is defined by how fast the server replies: a
slower backend is simply asked for less work. A service level is written against
the second question.

`benchmarks/run-app-baseline-rate-sweep` holds the arrival rate fixed using the
harness's existing Poisson open-loop generator and lets latency move. It shells
out to a language runner once per rate rather than adding a rate dimension
inside the harness, so the frozen reporting path is untouched. A rate the
backend cannot absorb appears as `client_queue_rejected` and a growing
`queue_delay`, which is the point: the curve shows where a service level stops
holding.

## Known limitations

- **Comment bodies stop at 256 bytes.** `comment.body` is `string<256>` in
  `ticketdesk.riff`; seeding above it fails the command with `RDB-INPUT-0101`
  rather than truncating. Real help desk comments are much longer. Carrying that
  would mean widening the contract, which also defines the frozen comparator
  dataset and every generated client, so it is out of scope here. This is the
  largest remaining realism gap.
- **No full-text search.** Arguably the most-used help desk query, and the
  workload cannot express it. It is also the surface RiffDB differentiates on
  (the exact-text provider, ADR-0130), so its absence understates RiffDB rather
  than flattering it. Add a search scenario once that provider lands.
- **The packed column path is unused.** `PackedQueryResult` exists and no
  scenario negotiates it, so the columnar plane is untested at any scale.
- **No attachments, SLA timers, reporting aggregations, saved views with deep
  pagination, bulk operations, or email ingestion.**
- **Clean shutdown needs a much larger budget.** Graceful shutdown checkpoints
  the published journal suffix, and at this size that measured about 21 seconds
  against 16 ms for a second, already-checkpointed shutdown
  (`riffdb-shutdown-stages-v1` puts nearly all of it in the final stage). The
  harness budgeted 15 seconds, so it SIGKILLed the daemon mid-checkpoint, and
  the next daemon then exited without a ready line because it was opening a
  database left mid-write. Both are fixed here, but the operational point stands
  on its own: a 4.7 GB database takes tens of seconds to close cleanly, and
  nothing outside this profile measures that. Set
  `RIFFDB_STOP_TIMEOUT_SECS` generously for larger datasets.
- **Only the Rust harness seeds concurrently.** `examples/app-baseline/riffdb`
  seeds with `DEFAULT_SEED_CONCURRENCY: usize = 128`; the TypeScript harness
  seeds with a sequential `for`/`await` loop over one session, and Go and Python
  do the same. At `full` that difference is invisible because the seed is 19,220
  commands. At `production` it is roughly 723,000 commands, and a sequential
  seed runs at single-client rate: a measured TypeScript run reached 1.4 GB of
  durable data after twenty-one minutes and had not finished. Use the Rust
  harness for production-scale runs until the other three seed concurrently.
  This also means seed *time* is not comparable across harnesses, which matters
  because the seed ceiling is a `PERF-008` metric; it is stated against the
  frozen Rust comparator, so the gate itself is unaffected.
- **Four independent scale definitions.** The Rust core, TypeScript, Go, and
  Python harnesses each carry their own copy of these numbers, and the flag was
  silently ignored by three of them until this change. Nothing proves they stay
  in agreement; a first run that seeds in two seconds instead of minutes is the
  only current symptom. This mirrors the duplication ADR-0148 addresses for the
  drivers and deserves the same treatment.

## Confirmed defect: startup indexes every command ever executed

`--seed-only` completes cleanly at this tier: 1,112,350 commands in 192 s at
5,794 ops/s, a clean 21 s shutdown, exit 0. The measured process then cannot
start against the seeded database, and the harness reports:

```text
app-baseline failed: riffdbd process failed: server stdout closed before ready
```

### Root cause

Startup builds a historical evidence plan and refuses when its in-memory index
exceeds `MAX_STARTUP_EVIDENCE_INDEX_BYTES`, 512 MiB, in
`crates/riffdb-storage-redb/src/startup.rs`. Tracing each collector that feeds
`build_historical_evidence_plan`:

```text
plan after bundles:          rows=1
plan after migration_edges:  rows=1
plan after plans:            rows=1112351     <- one locator per command, ever
plan after active_catalog:   rows=1112352
LIMIT site=evidence_plan index=536871037
```

`collect_plan_locators` inserts **one locator per historical command**. Startup
memory and time therefore grow linearly with every command the database has
ever executed, and past roughly a million lifetime commands the index crosses
512 MiB and the database will not open. For a help desk at this profile's rate
that is days to weeks of operation.

The failure surfaces as `CatalogErrorKind::Storage`, because the plan is built
under catalog resolution.

### The validated-prefix checkpoint is NOT the problem

An earlier revision of this document blamed the ADR-0085 validated-prefix
checkpoint, on evidence that turned out to be a measurement artefact: the
database inspected had been copied mid-run, after the seed but before the
post-seed checkpoint write, so its stale `commit_sequence = 0` was the expected
state of that snapshot rather than a defect.

Traced end to end on the real path, the checkpoint mechanism works exactly as
designed:

```text
checkpoint IGNORED reason=Absent      <- first boot, empty database
checkpoint OK seq=0                   <- correct for an empty predecessor
barrier before: commits_rows=19856
barrier after:  commits_rows=19878    <- suffix materialised at shutdown
checkpoint OK seq=1112350             <- full head recorded and reloaded
```

`ensure_command_cache` is bounded by that checkpoint and does not blow its
budget. `build_historical_evidence_plan` is a separate index that the checkpoint
does not bound at all, which is why a working checkpoint does not save the boot.

### The fix, for review

Bound plan-locator collection by the validated-prefix checkpoint, exactly as
`ensure_command_cache` already is: the checkpoint attests that the prefix was
validated, so only the suffix needs locators. The pattern is established in the
same file, and `build_historical_evidence_plan` already receives
`StartupValidationInputs`.

That is not applied here because it changes what startup trusts. Whether the
checkpoint's attestation covers the historical *evidence* the plan proves --
`InvalidHistoricalEvidence` names evidence that is "incomplete, reordered,
repeated, or misbound" -- is an ADR-0085 semantics question. If it does, this is
a small change to a known pattern. If it does not, bounding the collection would
weaken corruption detection to make a boot succeed, which is the wrong trade.

What is fixed here is the diagnostic. `kind=startup` alone named neither the
subsystem nor the class; a startup refusal now names the closed `StorageErrorKind`,
`StartupIntegrityFailure`, or `CatalogErrorKind` discriminant. Finding this
defect took five instrumentation cycles, three of which existed only to learn
which of those three it was.

## What this does not change

`smoke` and `full` are untouched, including their row counts, weights, board
density and payload bytes. `PERF-018` freezes the comparator dataset and
weights; every banked receipt, the `PERF-008` comparative gates, and the seed
ceiling are stated against `full`, and a production cell is reported under its
own scale name so the two cannot be conflated.
