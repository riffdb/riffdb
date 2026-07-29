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

## Run

From the repository root:

```bash
# smoke (small dataset, few samples)
./benchmarks/run-app-baseline --smoke

# larger baseline (thousands of rows)
./benchmarks/run-app-baseline --full
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
- RiffDB seed and mutations use **symbolic application commands** (no field IDs).
- RiffDB reads use **named RiffQL queries** (no `GetEntity` / `ScanIndex` in app code).
- PostgreSQL stays in the nested comparison workspace only.

## Scale

| Profile | Approx rows |
|---------|-------------|
| `--smoke` | ~200 |
| `--full` | ~thousands (10 orgs × projects × tickets + comments/labels) |
