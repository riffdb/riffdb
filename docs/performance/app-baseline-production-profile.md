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

Production seed throughput, 1,112,350 commands:

| Host | Harness | Seed time | ops/s |
| --- | --- | ---: | ---: |
| N1 | Rust | 492 s | 2,259 |
| E2 | TypeScript | 674 s | 1,650 |
| E2 | Go | 716 s | 1,554 |

Before the batched seed landed, a TypeScript production seed reached 1.4 GB
after twenty-one minutes and had not finished; it now completes in eleven.

### Not yet measured

- **No same-host production pair.** N1 has the production Postgres number and
  E2 has the production RiffDB numbers. The RiffDB production number on N1 is
  blocked by the startup defect below.
- **No workstation cells.** Deliberately: the host was busy, and the host note
  explains why its numbers would not transfer anyway.
- **Single rep, single tenant, one concurrency point.** Nothing here bounds
  run-to-run variance, and an earlier session saw Postgres swing 17% between
  identical TypeScript runs.

## Open defect: the clean-close fast path does not engage

A `riffdbd` started against this tier's cleanly shut down database takes the
complete startup validation pass: more than twenty minutes at 99.9% CPU and
about 10 GB resident on N1, without becoming ready, and with essentially no
physical reads (12 KB), so it is CPU work over page-cached data. A `perf` flat
profile puts 16.8% in `sha2::sha256::soft::unroll::compress` plus
`riffdb_proto::wire::Cursor::next`, `durable_wire::preflight`, `crc32` and
`prost` varint decoding -- a full re-parse and re-validation of every durable
record, which is what ADR-0156 exists to avoid on a clean start.

One contributing cause is fixed here: `restart_for_measurement` took its
pre-measurement inventory through `authoritative_table_inventory_after_reopen_v1`,
which opens a full `RedbStore` and drops it. Opening transitions the ADR-0157
lifecycle record to dirty and nothing in `Drop` writes it back, so that reopen
discarded the certificate the seed daemon's shutdown had just written. Every
measured restart therefore took the complete pass, including the run previously
cited in this document as verifying that ADR-0156 resolved the startup ceiling --
that claim was wrong and is withdrawn. The call site now uses the read-only
`authoritative_table_inventory_v1`.

That fix was not sufficient: the restart is still slow. The gate is a single
`verified_clean_close_lifecycle` check (`startup.rs`, admitted at the
`clean_close_fast: true` construction) which returns `None` silently on any of
eight preconditions, with no reason code and no counter, so the eight cannot
currently be told apart without patching the engine. A reason code is the next
deliverable, ahead of any change to admission logic.

## Known limitations

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

The `LimitExceeded` refusal is gone: the production cell reaches its load phase
and completes, which it could not before. But the claim that once stood here --
that this verified the ADR-0156 fast path -- was wrong. That run took the
complete validation pass, because the harness reopened and dirtied the database
between the clean shutdown and the measured start. See the open defect above.

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
