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

`--production` is accepted by every language runner and, since this profile
first ran, by `benchmarks/run-app-baseline` -- the Rust runner, and the only one
that stands up the Postgres comparator. Until that arm existed a production run
had to be `--skip-postgres` against the harness binary directly, so a
production-scale RiffDB-versus-Postgres comparison was not expressible.

```bash
benchmarks/run-app-baseline --production \
  --load interactive --load-clients 32 --load-duration-secs 30 --reps 1
```

Reports carry `scale: "production"`, so a production cell cannot be mistaken
for a `full` one.

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

## First measured cells

All non-evidentiary. Run on idle GCP VMs rather than the workstation, because
the workstation was carrying a load average near six and, as the host note
below records, its hardware is the least representative of the three.

Hosts, both 8 vCPU:

| Host | CPU | `sha_ni` | fsync p50 (4 KiB) |
| --- | --- | --- | ---: |
| N1 | Intel Xeon @ 2.30GHz | no | 1.543 ms |
| E2 | AMD EPYC 7B12 | yes | 2.601 ms |

`interactive` profile, `--load-clients 32`, 60 s window, one rep, single tenant.

| Host | Language | Scale | Backend | ops/s | p50 | p95 | p99 | errors |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| N1 | Rust | full | Postgres | 32,592 | 0.508 ms | 3.539 ms | 6.029 ms | 0 |
| N1 | Rust | full | RiffDB | 7,259 | 1.769 ms | 20.972 ms | 28.312 ms | 0 |
| N1 | Rust | production | Postgres | 30,577 | 0.557 ms | 3.801 ms | 6.291 ms | 0 |
| N1 | Rust | production | RiffDB | 6,359 | 2.097 ms | 24.117 ms | 33.554 ms | 0 |
| E2 | Rust | production | Postgres | 29,978 | 0.508 ms | 4.063 ms | 6.554 ms | 0 |
| E2 | Rust | production | RiffDB | 5,225 | 2.490 ms | 29.360 ms | 39.846 ms | 0 |
| E2 | TypeScript | production | RiffDB | 4,235 | 5.767 ms | 20.972 ms | 29.360 ms | 0 |
| E2 | Go | production | RiffDB | 2,742 | 9.437 ms | 29.360 ms | 39.846 ms | 0 |
| N1 | Python | production | RiffDB | 1,691 | 11.534 ms | 58.720 ms | 100.663 ms | 0 |

Three same-host, same-harness pairs: **RiffDB reaches 0.223x Postgres at `full`
on N1, 0.208x at `production` on N1, and 0.174x at `production` on E2**. That is
the shape of the long-standing "wins on the workstation, loses on a GCP VM"
report, now reproduced on two quiet hosts.

E2 has SHA-NI and N1 does not, and E2 is the **worse** of the two for RiffDB.
So hardware SHA-256 does not explain the serving gap -- it explains the startup
cost and nothing else. What does track is fsync: E2's durable append is 1.7x
slower than N1's (2.601 ms against 1.543 ms) and its RiffDB write latency is
correspondingly higher (`create_comment` p50 25.166 ms against 20.972 ms) while
its Postgres numbers are unchanged. A load-phase `perf` profile of `riffdbd`
agrees that the serving path is not hashing: it is dominated by libc
`malloc`/`free` plus drop glue for `CanonicalValue`, `CommandDerivedIndexes` and
`StoredProvenanceRecordV1`.

The ratio is almost unchanged between the tiers, and the per-scenario breakdown
says why: the gap is in the write path, not in reads or in data size.

| Scenario (N1, production, c=32) | Postgres p50 | RiffDB p50 | RiffDB / PG |
| --- | ---: | ---: | ---: |
| point_get_ticket | 0.410 ms | 1.704 ms | 4.2x |
| list_comments_for_ticket | 0.459 ms | 1.835 ms | 4.0x |
| ticket_detail_page | 2.228 ms | 2.228 ms | 1.0x |
| create_comment | 2.753 ms | 20.972 ms | **7.6x** |
| close_ticket_with_comment | 4.981 ms | 20.972 ms | 4.2x |
| open_ticket_with_labels | 4.981 ms | 22.020 ms | 4.4x |

Reads are a consistent 4x behind and the composite read page is level. Writes
are 21 ms at p50 against Postgres's 2.8 ms, and `interactive` is a quarter
writes, so the write path sets the aggregate.

Postgres barely moves between `full` and `production` on N1 (32,592 to 30,577,
a 6% decline for 76x the rows), and E2 independently reports 29,978 for the same
production Postgres cell. At these sizes the tier does not stress either
engine's storage much -- see the working-set note under limitations.

**The Rust cells and the language cells are not measured the same way.** Only
`examples/app-baseline/src/main.rs` calls `restart_for_measurement`; the
TypeScript, Go and Python harnesses load against the same daemon that just
performed the seed. So the Rust rows measure a freshly started process and the
other three measure a warm one that has just written 1.1M records. If anything
that flatters the language rows, and they are still well below Rust -- 4,235,
2,742 and 1,691 against 6,359 -- so the binding cost dominates the difference
rather than daemon warmth. Do not read a language row against a Rust row as a
storage comparison.

Production seed throughput, 1,112,350 commands:

| Host | Harness | Seed time | ops/s |
| --- | --- | ---: | ---: |
| N1 | Rust | 492 s | 2,259 |
| E2 | TypeScript | 674 s | 1,650 |
| E2 | Go | 716 s | 1,554 |
| N1 | Python | 574 s | 1,938 |

Before the batched seed landed, a TypeScript production seed reached 1.4 GB
after twenty-one minutes and had not finished; it now completes in eleven. All
four harnesses now complete a production seed, which is the point of the change.

Python is the fastest of the three non-Rust harnesses despite being the slowest
under load, which is consistent with where each spends its time: its runtime
batch dispatches per item across a thread pool that releases the GIL for each
native round trip, while TypeScript and Go fan out over `driverd` and pay
framing per batch.

### Concurrency curves

`--load-concurrency-sweep` at c=1/8/32/128, 30 s windows, one rep. These are the
cells the ratio discussion above rests on, recorded here because the aggregate
ratio hides the shape.

`full` scale on N1:

| clients | Postgres ops/s | RiffDB ops/s | ratio |
| ---: | ---: | ---: | ---: |
| 1 | 3,655 | 815 | 0.223x |
| 8 | 20,519 | 4,846 | 0.236x |
| 32 | 32,254 | 7,292 | 0.226x |
| 128 | 21,046 | 7,675 | 0.365x |

`production` scale on E2:

| clients | Postgres ops/s | RiffDB ops/s | ratio | RiffDB ops completed |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 2,216 | 401 | 0.181x | 12,025 |
| 8 | 16,669 | 3,230 | 0.194x | 96,930 |
| 32 | 28,027 | 5,448 | 0.194x | 163,562 |
| 128 | 16,253 | 7,067 | 0.435x | **212,404** |

Every cell above completed with zero errors, zero conflicts and zero idempotency
mismatches.

Two readings matter and they differ by scale. At `full` RiffDB is flat from c=32
to c=128 (7,292 to 7,675, +5%); at `production` it still climbs (5,448 to 7,067,
+30%), so group commit does amortise with concurrency once the dataset is large
enough that the single writer is not already the whole story. But in both cases
Postgres is past its own knee at c=128 -- dropping 35% on N1 and 42% on E2, with
an 11.5-second maximum latency in the `full` run -- so roughly half of each
ratio "improvement" at c=128 is Postgres degrading rather than RiffDB gaining.
Neither c=128 cell should be read as a win.

The durable finding is the c=1 column: **0.18-0.22x with a single client**, no
concurrency, no contention and no queueing. A constant factor that exists before
any concurrency effect cannot be closed by concurrency scaling. Note also that
the c=1 read figures were measured while the columnar worker was consuming ~45%
of CPU draining seed backlog, so the read-side component of that constant is not
yet trustworthy and is being re-measured.

The `production` c=128 count of 212,404 operations at zero errors is cited in
ADR-0105 Amendment 1 as counter-evidence that this repository triggers h2 issue
#939 in practice.

### What a run costs

Fully attributed by polling process state every 15 s during a `--production`
RiffDB-only run on N1, for a 13-second measurement window:

| Phase | Wall clock |
| --- | ---: |
| Seed, 1,112,350 commands | 7.8 min |
| Seed daemon graceful shutdown | 12.3 min |
| Read-only pre-measurement inventory | ~15 s |
| Measured daemon readiness | 21.8 min |
| Load window plus post-measurement evidence | ~1 min |
| **Total** | **43.6 min** |

Two phases are 78% of it, and both have named causes above. Note the
post-measurement inventory is cheap despite opening a full `RedbStore`: that
open takes the bounded path too and builds no graph, so it never reaches
`recover_outbox`. Budget an hour per production cell until that is fixed.

### Not yet measured

- **No same-host production pair.** N1 has the production Postgres number and
  E2 has the production RiffDB numbers. The RiffDB production number on N1 is
  blocked by the startup defect below.
- **No workstation cells.** Deliberately: the host was busy, and the host note
  explains why its numbers would not transfer anyway.
- **Single rep, single tenant, one concurrency point.** Nothing here bounds
  run-to-run variance, and an earlier session saw Postgres swing 17% between
  identical TypeScript runs.

## Open defect: readiness is 96% outbox recovery

A `riffdbd` started against this tier's cleanly shut down database takes about
22 minutes to become ready on an N1 VM. The ADR-0156 clean-close fast path
**does** engage -- `clean_close_fast=true`, confirmed twice at the full
1,112,350-command scale -- and the readiness cost is almost entirely somewhere
ADR-0156 does not bound.

`riffdb-startup-stages-v1` on a bounded measured restart at 115,690 retained
commands, microseconds:

```text
store_open=1318215  evidence_begin=1173  structural_drain=156  catalog_history=3353
evidence_finish=14710  port_activation=0  current_views=90  consumer_recovery=6
outbox_recovery=31980581  graph_rest=10051  process_to_ready=33348476
```

`outbox_recovery` is 31.98 s of 33.35 s. Everything ADR-0156 bounds --
evidence begin, structural drain, catalog history, evidence finish -- is 0.02 s
combined, and `port_activation=0` shows the intended saving is real.

The mechanism: `activate_operational_ports` correctly returns early when
`bounded_clean_startup` is set, but `ProductionGraphBuilder::build` then calls
`recover_outbox` unconditionally, whose first storage call reaches
`ensure_transient_indexes_ready` and performs exactly the population rebuild
activation had just skipped -- decoding every `COMMITS` row, cloning and
retaining every segment, re-deriving every manifest key, then walking every
event of every command. That is what the earlier `perf` profile was showing:
SHA-256 for manifest-key derivation, `wire::Cursor::next`, `durable_wire::
preflight`, CRC32 and prost varint for per-segment decode, `malloc`/`free` for
the per-segment clones, and ~10 GB resident for the retained segments.

On the bounded path the result is then **discarded**: the outbox is declared
`Degraded` and `refresh` is skipped precisely because caches are cold. So the
bounded path pays a full rebuild for a value it throws away.

The cost is linear and tightly reproducible -- about 283 µs per retained
command, 3.17x across a 3.11x data increase -- so it is a constant factor, not
an algorithmic problem. Extrapolating to 1,112,350 commands and scaling for host
seed rate predicts 19.8 minutes against 21.8 observed.

A fix is not yet applied. Skipping `recover_outbox` on the bounded path, or
deferring it past readiness, changes when outbox `Delivering` statuses are
normalized, which is an ADR-0156/ADR-0157 question about the "cold caches"
claim and outbox delivery guarantees rather than an implementation detail. It
needs the maintainer's exact-text acceptance.

### Withdrawn: the clean-close gate

An earlier revision of this document claimed the fast path did not engage. That
was an inference from a slow restart, not an observation, and it was wrong. What
made it decidable was adding a reason code: `verified_clean_close_lifecycle`
previously returned a bare `None` on any of ten preconditions, so every one of
them looked identical from outside -- a slow start. It now names the declining
precondition. A separate real defect was fixed on the way: the harness's
pre-measurement inventory opened a full `RedbStore` and dropped it, which marks
the lifecycle record dirty with nothing writing it back, so the measured daemon
was forced onto the complete pass. That window is now about 15 seconds.

## Clean shutdown: 12 minutes, and it is not the checkpoint

A graceful stop of this database measured 12.3 minutes at about 180% CPU and
11.9 GB resident. An earlier revision of this document attributed roughly 21
seconds to the ADR-0019 A1 validated-prefix checkpoint, generalised from a
smaller dataset. Both the number and the attribution were wrong.

Labelled stage timings across two seed-daemon shutdowns at identical size,
same binary and workload:

| Stage | run A | run B |
| --- | ---: | ---: |
| `columnar_worker` | **36.63 s** | **0.137 s** |
| `validated_prefix_checkpoint_write` (legacy label) | 0.183 s | 0.183 s |
| `post_graph_release` (contains `redb::Database::drop`) | 0.044 s | 0.034 s |

The checkpoint write is 0.183 s and flat at every size, so it is ruled out, as
is redb's drop-time close. The cost is the columnar worker draining its ingest
backlog, and it varies 267x at the same data size -- it is a queue depth at the
moment of shutdown, not a function of history. The seeder outruns the columnar
worker, and shutdown pays whatever is outstanding. The lever is ingest
backpressure, not an algorithm, and no scaling law should be quoted from two
points that differ by 267x.

Two reporting gaps that hid this, both now closed: the harness's
`graph_shutdown_elapsed_us` records the *final* daemon rather than the seed
daemon (45-68 ms against 36.8 s in the same run), and
`riffdb-shutdown-stages-v1` is a positional unlabelled line emitted once per
daemon, so three lines from three daemons were indistinguishable. That is why an
earlier reading of 758 ms coexisted with a 12-minute observed shutdown.

## Known limitations## Known limitations

- **The load phase reads one organization out of two hundred.** `--load-tenants`
  defaults to 1 and is capped at 64, and a tenant is an organization, so every
  cell above reports `tenant=single_organization`. At `production` that working
  set is roughly 600 tickets and 3,000 comments -- *smaller* than the whole
  `full` dataset -- sitting inside a database 76x larger. So the tier does
  measure deeper B-trees, a larger journal, more page-cache pressure and writes
  appending into a bigger store, but it does not measure a production-sized
  working set, and the near-flat Postgres result across the two tiers is what
  that looks like. Run `--load-tenants 64` for a wider set; that cell is not
  measured yet, and it is the most important gap in this section.

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
- **Clean shutdown still needs a measured budget.** The final stage now drains
  any published journal suffix, classifies retained validated-prefix evidence
  without rewriting or walking it, and commits only CLEAN. Its cost is bounded
  by the retained suffix and fixed metadata, not database population. Worker
  backlog and engine close can still dominate wall time, so configure
  `RIFFDB_STOP_TIMEOUT_SECS` from lifecycle evidence rather than database size.
- **Seed time is not comparable across harnesses.** All four now seed
  concurrently -- the Rust harness with `DEFAULT_SEED_CONCURRENCY: usize = 128`,
  and TypeScript, Go and Python through their generated bounded batch envelopes
  at the same concurrency and the same 4,096-row chunking. But they reach that
  concurrency by different means: the Rust harness fans out inside one async
  runtime, the TypeScript and Go clients fan out over `driverd`, and the Python
  runtime fans out over a thread pool whose per-item work still contends for the
  GIL. Seed throughput therefore measures the binding as much as the store. The
  `PERF-008` seed ceiling is stated against the frozen Rust comparator, so the
  gate itself is unaffected.
- **The three available hosts differ in two ways that pull in opposite
  directions**, so a RiffDB-versus-Postgres ratio does not transfer between
  them. SHA-256 throughput, measured with the same `sha2` 0.11 crate and feature
  set the daemon uses, 256-byte inputs: workstation (Ryzen 9 7950X, `sha_ni`)
  7,410,788 hash/s; E2 (EPYC 7B12, `sha_ni`) 4,779,870; N1 (Xeon @2.30GHz, no
  `sha_ni`) 476,597 -- N1 is 15.5x slower than the workstation. This is a CPU
  capability difference, not a build flag: Intel server parts before Ice Lake
  lack SHA-NI and every AMD Zen part has it, so GCP AMD machine types should
  behave like E2. Durable append (4 KiB write plus fsync) runs the other way:
  workstation 5.218 ms p50, E2 2.601 ms, N1 1.543 ms -- the workstation has the
  **slowest** commit path of the three, by 3.4x. Moving from the workstation to
  a GCP VM therefore makes the commit path faster and the hashing path much
  slower at the same time. State the host with every number.

- **Four independent scale definitions.** The Rust core, TypeScript, Go, and
  Python harnesses each carry their own copy of these numbers, and the flag was
  silently ignored by three of them until this change. Nothing proves they stay
  in agreement; a first run that seeds in two seconds instead of minutes is the
  only current symptom. This mirrors the duplication ADR-0148 addresses for the
  drivers and deserves the same treatment.

## Resolved: the startup ceiling (ADR-0156 / ADR-0157)

This profile's first run could not start the measured process against its own
cleanly seeded database. Startup built a paginated historical evidence plan but
materialised the whole ordered locator list up front, and refused once that index
exceeded `MAX_STARTUP_EVIDENCE_INDEX_BYTES`, 512 MiB. Measured attribution, per
collector inside `build_historical_evidence_plan`:

```text
after bundle_locators:                  distinct=1   bytes=85
after contract_migration_edge_locators: distinct=1   bytes=85
after plan_locators:                    distinct=9   bytes=1565
after active_catalog_locator:           distinct=10  bytes=1629
                                        (never reaches the next line)
LIMIT site=evidence_plan index=536871037
```

The catalog-shaped collectors were trivial. The whole budget went to
`collect_persisted_key_locators`, which walked `ENTITIES` and
`SECONDARY_INDEXES` and retained one locator per row at roughly 488 charged
bytes each. The ceiling therefore scaled with live data size, not history, so
retention could not relieve it: a database over roughly a million rows would not
open.

ADR-0156 and ADR-0157 resolve it with a durable clean-close certificate. A
process that shut down gracefully records one private versioned lifecycle
record, and the next start admits a bounded readiness path instead of the
complete pass; anything dirty, uncertain, migrated, restored or contradictory
still takes complete validation and recovery. That amends ADR-0019's
unconditional complete-pass rule, ADR-0073's rejection of clean-shutdown
markers, and ADR-0085's rejection of a validation-free clean fast path, and it
was accepted with exact maintainer text.

The clean-certificate path avoids that population materialization, but the
complete path has not removed the ceiling. A one-shot dirty production-scale
attempt still refused with typed `LimitExceeded` at the compiled 512 MiB
historical-evidence bound and did not publish readiness. It is retained as a
fail-closed handoff, not a passing startup or memory result. Clean production
qualification and dirty complete-path qualification are therefore separate
receipts; success in the former must never be described as removing the latter
ceiling.

Two notes worth keeping with the profile:

- ADR-0156 records that latent corruption outside the bounded readiness roots
  may now be detected after readiness, when the affected data is accessed or
  during an explicit scrub, rather than at startup. That is the trade the fast
  path makes.
- The memory-bounded historical cursor remains required rather than becoming
  dead code, so the underlying materialisation cost still applies whenever the
  complete path runs -- which is every dirty start.

### Readiness budget

Because a start that cannot use the certificate still pays the complete pass,
readiness at this size can exceed a minute. The harness budgeted ninety seconds
and reported `ready timeout` for a daemon that was starting correctly; it now
budgets 600 s with a `RIFFDB_START_TIMEOUT_SECS` override, matching the
`RIFFDB_STOP_TIMEOUT_SECS` treatment the shutdown budget already needed.

## What this does not change

`smoke` and `full` are untouched, including their row counts, weights, board
density and payload bytes. `PERF-018` freezes the comparator dataset and
weights; every banked receipt, the `PERF-008` comparative gates, and the seed
ceiling are stated against `full`, and a production cell is reported under its
own scale name so the two cannot be conflated.
