# TicketDesk App Baseline

Application-shaped performance baseline comparing:

| Side | Path |
|------|------|
| **PostgreSQL** | Live server, SQL schema with 8 tables, joins + indexes |
| **RiffDB** | Live **`riffdbd`**, TicketDesk contract, public **gRPC** only |

This is not the frozen WP-200 budget-comparison suite. It is an optimization
baseline for multi-table application access patterns (point lookups, filters,
detail pages). RiffDB is not expected to “win” SQL join microbenchmarks; the
goal is a repeatable starting point.

## Domain

TicketDesk (8 tables / entities):

1. `organization`
2. `app_user`
3. `project`
4. `project_member`
5. `ticket`
6. `comment`
7. `label`
8. `ticket_label`

Contract source: `contracts/ticketdesk.riff` (deployed to `riffdbd` at runtime).

## Scenarios

| Scenario | Postgres | RiffDB (application-equivalent) |
|----------|----------|----------------------------------|
| `point_get_ticket` | `SELECT` by PK | named `GetTicket` |
| `point_get_user` | `SELECT` by PK | named `GetUser` |
| `list_tickets_by_project_status` | filtered `SELECT` | named `ListTickets` |
| `list_open_tickets_for_assignee` | filtered `SELECT` | named `ListTicketsByAssignee` |
| `list_comments_for_ticket` | filtered `SELECT` | named `ListComments` |
| `list_project_members` | filtered `SELECT` | named `ProjectMembers` |
| `ticket_detail_page` | multi-table `JOIN` | named `TicketPage` (dependent key batches) |
| `create_comment` | plain `INSERT` of a distinct new row per sample | symbolic `CreateComment` |
| `close_ticket_with_comment` | SQL txn: validate + `UPDATE` ticket + `INSERT` comment | one symbolic `CloseTicketWithComment` (mutate + create) |
| `swap_member_roles` | SQL txn: two `UPDATE` memberships | one symbolic `SwapMemberRoles` (two mutates) |
| `open_ticket_with_labels` | SQL txn: ticket + two label links | one symbolic `OpenTicketWithLabels` (three creates) |

## Run

From the repository root:

```bash
# smoke (small dataset, few samples)
./benchmarks/run-app-baseline --smoke

# larger baseline (thousands of rows)
./benchmarks/run-app-baseline --full

# fail unless the same-run seed and every write p50 are within 2x PostgreSQL
./benchmarks/run-app-baseline --full --assert-write-parity

# concurrent mixed-workload load driver (closed-loop; realistic tails/contention)
./benchmarks/run-app-baseline --smoke --load interactive --load-clients 8 \
  --load-duration-secs 5 --load-warmup-secs 1

./benchmarks/run-app-baseline --full --load agent --load-clients 32 \
  --load-duration-secs 30 --load-warmup-secs 5

# ordinary concurrent reads and writes on the same hot ticket
./benchmarks/run-app-baseline --smoke --load interactive --load-clients 8 \
  --load-duration-secs 5 --load-warmup-secs 1 --load-contended
```

### Load driver (`--load`)

The parity suite answers warm single-client p50 vs Postgres. The load driver
answers concurrency, hot-key contention, and outcome mix:

| Profile | Mix (approx) | Pacing |
|---------|--------------|--------|
| `interactive` | ~85% reads / ~12% single writes / ~3% multi-entity | continuous closed-loop; **no** `SwapMemberRoles` |
| `agent` | more multi-entity + detail pages | bursts of 8 ops + 8 ms think-time; ~5% intentional comment replays; **no** `SwapMemberRoles` |
| `membership_contention` | hammers `SwapMemberRoles` on the shared membership pair | continuous; **not** isolated under concurrency (intentional) |

By default, each client owns an independent connection: one RiffDB HTTP/2
connection or one PostgreSQL TCP connection. This avoids comparing a single
multiplexed RiffDB socket with many PostgreSQL sockets. The shared RiffDB
channel shape remains available with `--load-riffdb-transport shared`; the
exact topology is recorded in report JSON. PostgreSQL capacity is read from
the live server and an excessive client request is rejected rather than
silently clamped. RiffDB is bounded at 128. Each handle is **prewarmed**
(Postgres prepares every timed statement; RiffDB touches a cheap read) before
the shared measure window.

Open tickets for comment/close writes are selected with Zipf skew via a CDF
`partition_point` (`--load-zipf-s`, default 1.0). Read probes use a separate
stable ticket excluded from that pool. All load traffic targets a **single
organization** partition (`tenant_scope=single_organization` in the report).

Measurement uses a **shared `AtomicBool` stop**: coordinator opens `measuring`
after warmup and sets `stop` after `duration`. Throughput denominator is
**max(worker measure end) − min(worker measure start)** (exact, not sleep
approximate). Each worker also reports its own span
(`worker_measure_intervals_ns`).

`CreateComment` and `CloseTicketWithComment` both use the Zipf write-ticket
pool (re-close is valid; **not** single-ticket serialized). The pool is a
seed-time Open snapshot — closes flip status mid-run without open-status
requires today. Intentional comment replays match across backends: RiffDB
same-key equal-input replay and a distinct PostgreSQL replay operation using
`ON CONFLICT DO NOTHING`. Ordinary PostgreSQL creates remain strict. A replay
draw before a worker has completed a create is executed and counted as fresh.

Outcomes classify via **RiffDB `RDB-*` codes** or **PostgreSQL SQLSTATE**,
never prose matching. `unavailable` is separate from application `conflict`
and makes the load run fail after its report is written. Latencies use a fixed
log-linear histogram with 16 buckets per power-of-two octave (at most 6.25%
quantization; p50/p95/p99). Outcomes: `success`, `conflict`, `unavailable`,
`idempotency_mismatch`, `replayed`, `error`. An idempotency mismatch is a
harness/application correctness bug, not a contention conflict. RiffDB reports
also attach the server
write-completion-group histogram after shutdown.

Automatic RiffDB command retry is disabled for load runs
(`command_attempt_budget=1`), so one logical operation is one transport
submission and conflict latency/counts cannot hide retries. Normal application
and parity paths retain their configured uncertainty-recovery policy.

The default isolates writes from the stable read-probe ticket. Pass
`--load-contended` to retain the ordinary “view while someone comments” stress.
A public `RDB-STORAGE-0101` is reported as `unavailable` and fails the run; the
harness never routes around it.

The first contended evidence run exposed a specific open product issue. Once
the hot ticket exceeds the 50-row comment page, every ignored first-page
continuation occupies one reusable cursor slot for five minutes. The accepted
cursor contract permits 64 live cursors per principal, returns unavailable on
exhaustion, and forbids eviction. Repeated page-one refreshes therefore
eventually fail even though redb's read snapshot is healthy. The contended mode
is the deterministic regression reproducer. Closing it requires an explicit
product contract—preferably a compatible first-page-only/no-continuation option
or a cursor-release operation—not benchmark key isolation or silent eviction.

Report schema: `riffdb.app-baseline-load-suite/v1` (default path
`target/app-baseline/load-report-v1.json`).

Follow-ups not in this increment: open-loop Poisson arrivals, history-growth
curves, crash-under-load, deploy-under-load.

Or directly after provisioning:

```bash
cargo +1.97.0 build --locked --release -p riffdb-server --bin riffdbd
cargo +1.97.0 build --release --manifest-path examples/app-baseline/Cargo.toml

export RIFFDB_APP_BASELINE_POSTGRES_URL=postgres://riffdb:riffdb@127.0.0.1:55432/riffdb_app_baseline
export RIFFDB_APP_BASELINE_RIFFDBD_BIN=$PWD/target/release/riffdbd

examples/app-baseline/target/release/riffdb-app-baseline \
  --smoke \
  --postgres-url "$RIFFDB_APP_BASELINE_POSTGRES_URL" \
  --riffdbd-bin "$RIFFDB_APP_BASELINE_RIFFDBD_BIN" \
  --output target/app-baseline/report-v1.json
```

The runner writes JSON to `target/app-baseline/report-v1.json` and prints a
human summary with p50 latencies and RiffDB/Postgres ratios.

## Architecture rules

- No raw `redb` / storage primitives on the RiffDB client path.
- RiffDB seed and mutations use **symbolic generated commands** (no field IDs).
- RiffDB reads use **named RiffQL queries** (no `GetEntity` / `ScanIndex` in app code).
- Runner credential is the compiled **`TicketDeskApplication` role** (manifest
  `fixtures/application-manifests/ticketdesk-v1.json`): exact command and named
  query names only; field visibility and scan ceilings are compiler-private.
- Application failures surface the **semantic application-error object** (code,
  category, recovery, operation), not kernel error payloads.
- RiffDB seed uses the public **bounded command-batch transport** over one
  HTTP/2 channel. Each exchange contains at most 16 ordinary commands and
  total in-flight work is bounded by `RIFFDB_SEED_CONCURRENCY` (default/max
  128). Every item retains independent authorization, idempotency, outcome,
  provenance, audit, and recovery semantics.
- PostgreSQL stays in the nested comparison workspace only.

## Scale

| Profile | Approx rows |
|---------|-------------|
| `--smoke` | ~276 |
| `--full` | ~15k (10 orgs × projects × tickets + comments/labels) |

## Seed performance investigation

The former full-scale collapse was not a deadlock. A service-audit append
decoded and validated the complete retained audit stream before every new row,
making seed work quadratic. WP-362 replaced that with allocator, table-length,
and decoded-tail validation; full validation remains on startup and reads.

WP-366 then removed fixed command-path amplification:

1. compiler-proven fine-grained mutation aggregates permit safe grouping;
2. active catalog and executable-plan material are cached under the exact
   durable active pointer;
3. terminal audit rows are staged as one validated physical group;
4. CRC-32C uses the existing safe 16-lane implementation; and
5. generated batches use the 16-item public transport with per-item recovery.

The full same-run evidence is recorded in
`docs/performance/wp-366-write-parity.md`. The seed is flat at roughly 3,900
ordinary commands/second rather than collapsing with retained history. All
interactive read and write p50s beat PostgreSQL in that run. The remaining
seed gap compares independently durable command lifecycles with PostgreSQL's
single 15,160-insert seed transaction and remains an explicit open gate.

Those figures predate the 2026-07-30 harness correction (per-sample distinct
write identities on both backends, plus a warm PostgreSQL connection and
prepared statements), so the interactive read/write numbers in that note must
be regenerated before being cited.
