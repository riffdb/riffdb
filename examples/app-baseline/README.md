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
| `create_comment` | idempotent `INSERT` | symbolic `CreateComment` |
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
```

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
  64). Every item retains independent authorization, idempotency, outcome,
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

The current full same-run evidence is recorded in
`docs/performance/wp-366-write-parity.md`. The seed is flat at roughly 3,900
ordinary commands/second rather than collapsing with retained history. All
interactive read and write p50s beat PostgreSQL in that run. The remaining
seed gap compares independently durable command lifecycles with PostgreSQL's
single 15,160-insert seed transaction and remains an explicit open gate.
