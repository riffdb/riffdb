# RiffDB

**Website:** [riffdb.com](https://riffdb.com) explains the product thesis and
hosts the managed-service early-access list. Its source and deployment notes are
in [`website/`](website/README.md). The managed service is not available yet;
the current implementation remains the standalone, local-only proof of concept
described below.

**Documentation:** Start with the [RiffDB Handbook](docs/README.md) for the
application tutorial, language guides, MCP cookbook, operations, architecture,
and reference material. The repository builds the same handbook locally with
`./scripts/handbook build` and publishes it through GitHub Pages from `main`.

## Build an application

The application-first path starts from symbolic contract and RiffQL sources:

```bash
riffdb new order-desk
cd order-desk
riffdb dev --seed
```

This produces and verifies typed Rust, TypeScript, Python, and MCP operations without
exposing numeric schema IDs, field masks, encoded keys, protobuf records, or
kernel RPCs to handwritten application code. See the
[agent application quickstart](docs/getting-started/AGENT-APPLICATION-QUICKSTART.md),
[contract reference](docs/contracts/README.md), and
[symbolic inspection guide](docs/getting-started/INSPECTION.md). Python
applications use the [Rust-backed Python driver](docs/python-driver.md).

RiffDB is a standalone database written in Rust for contract-first operational
state. Applications mutate state through compiled, typed commands. The database
owns invariant evaluation, conflict domains, retry identity, durable outcomes,
events, provenance, outbox intent, and projection frontiers.

This repository is a proof of concept, not a production release. The POC is
local-development safe: public endpoints are loopback-only, transport is
cleartext, and the authorization model uses server-side capability records. A
POC-exit claim is valid only after all three commands below pass from a clean
checkout and a maintainer records the architecture-review decision:

```bash
./scripts/ci-all
./scripts/demo --assert
./scripts/release-poc --verify
```

The checked-in evidence manifest is a candidate map, not a signed result.
`scripts/demo --assert` writes the revision-specific result to
`target/wp200/demo-report.json` only after every selected assertion succeeds.

## Symbolic application quick start

Application and agent development is RiffQL-first. Start a disposable,
authorized TicketDesk environment with its contract, named query module,
development role, and 276-row command seed:

```bash
cargo run -p riffdb-cli -- dev --seed
```

Every list or detail page is one symbolic query request; mutations remain one
compiled command invocation. The kernel gRPC API remains supported, but normal
application code does not construct IDs, masks, keys, or protobuf field maps.
See [Symbolic applications](docs/getting-started/SYMBOLIC-APPLICATIONS.md) and
the [safe application profiles](docs/getting-started/SAFE-APPLICATION-PROFILES.md),
and the [TicketDesk acceptance report](docs/getting-started/TICKETDESK-ACCEPTANCE.md).

## Why RiffDB

RiffDB narrows the application mutation surface. First-party gRPC, MCP, CLI,
and Rust SDK calls all enter the same API-neutral application service,
authorization layer, deterministic command runtime, conflict manager, and
commit coordinator. The commit coordinator is the only component that assigns
commit sequences or atomically persists authoritative mutations, outcomes,
events, provenance, and commit records.

The budget comparison keeps a correct PostgreSQL implementation beside RiffDB
and also demonstrates four deliberately unsafe PostgreSQL variants. The
qualification matters: a pattern described as impossible in RiffDB is absent
from RiffDB's **supported application mutation surface**. It is not a claim
about malicious administrators, filesystem compromise, defects, or features
outside the POC. Unsafe PostgreSQL variants are correctness evidence only and
are excluded from every performance result.

See [the comparison workspace](examples/budget-comparison/README.md) and its
checked [safety report](examples/budget-comparison/fixtures/safety/report-v1.json).

## Architecture

```text
riffdb / Rust SDK / riffdb-mcp       hosted MCP
               |                         |
       public gRPC adapter          MCP HTTP adapter
               \                         /
                       authentication
                              |
                API-neutral application service
                              |
                         authorization
                              |
               deterministic runtime + conflict manager
                              |
                       commit coordinator
                              |
                  authoritative redb storage
                              |
                 outbox and projection workers
```

MCP is a policy-filtered transport, never a storage path. Projection state is
derived and rebuildable; entity state and the commit log are authoritative.
The deterministic runtime has no filesystem, network, operating-system clock,
process-global mutation, or untracked randomness.

## Quick Source Install

From the repository root on a Linux systemd host, install a private user
service and then explicitly bootstrap its database:

```bash
cargo riffdb install --user
cargo riffdb bootstrap --user --register-codex
```

Omit `--register-codex` when Codex MCP registration is not wanted. A
machine-wide service uses `--system` for both commands. Run both commands as
the intended operator user: never run Cargo with `sudo`. The system installer
builds as that user and invokes `sudo` itself for privileged destination
inspection, publication, account setup, and systemd operations. Treat the
mutable checkout helper as an interactive administrator tool, not as a command
to allowlist in `sudoers`.

Install and bootstrap are intentionally separate. Bootstrap creates the first
durable administrator, creates a generic MCP developer capability, and writes
private client configuration. It does not deploy an application contract. The
developer can validate and deploy a contract; exact application command and
data access is granted afterward through application roles or scoped
capabilities. POC capabilities expire after at most 30 days and are not
automatically renewed. See [Installation](docs/installation.md) for paths,
`--no-start`, systemd user manager requirements, rerun limits, and advanced
bundle/manual procedures.

To install one daemon with multiple isolated databases, name them before the
first service start and bootstrap each alias independently:

```bash
cargo riffdb install --user --database ea --database orders
cargo riffdb bootstrap --user --database ea --register-codex
cargo riffdb bootstrap --user --database orders --register-codex
```

The generated MCP registrations are `riffdb_ea` and `riffdb_orders`; each
credential, client config, contract catalog, sequence, and durable file remains
bound to only its selected database.

## Manual Build

The workspace pins Rust 1.97.0. On Linux with rustup:

```bash
export PATH="${CARGO_HOME:-$HOME/.cargo}/bin:$PATH"
rustup show
cargo build --locked --release \
  -p riffdb-server --bin riffdbd \
  -p riffdb-cli --bin riffdb \
  -p riffdb-mcp-stdio --bin riffdb-mcp
```

Contributors running generated-artifact or handbook checks can install and
verify the exact auxiliary tools with:

```bash
./scripts/developer-tools install
./scripts/developer-tools check
```

The check reports a system `cargo` shadowing the rustup proxy and a Cargo
configuration that names an unavailable optional `rustc-wrapper`. It never
rewrites global Cargo configuration.

The POC ships exactly three binaries:

- `riffdbd`: the database server and optional hosted loopback MCP endpoint.
- `riffdb`: the public gRPC administration and application CLI.
- `riffdb-mcp`: a stdio MCP bridge over the ordinary public gRPC client.

Comparison and safety runners under `examples/` are evidence tools, not product
binaries.

## Run

For a disposable source-tree smoke test that uses the production server binary,
closed stdin, and SIGTERM:

```bash
cargo build --locked -p riffdb-server --bin riffdbd
./scripts/release-systemd-smoke --binary target/debug/riffdbd
```

For an installed service, key provisioning, bootstrap, and a first contract
deployment, follow [Installation](docs/installation.md). The exact server,
client, hosted MCP, and stdio bridge settings are in
[Configuration](docs/configuration.md).

One daemon may host up to 32 isolated named databases. Select one on every
public client:

```bash
riffdb --database ea --config "$HOME/.config/riffdb/client.toml" server health
riffdb-mcp --database ea --config "$HOME/.config/riffdb/mcp.toml"
```

For an MCP host using project `.mcp.json`:

```json
{
  "mcpServers": {
    "riffdb_ea": {
      "command": "/home/you/.local/bin/riffdb-mcp",
      "args": [
        "--config",
        "/home/you/.config/riffdb/mcp.toml",
        "--database",
        "ea"
      ]
    }
  }
}
```

Each alias has its own contract catalog, credentials, command history, commit
sequence, projections, outbox, and storage. There are no cross-database
operations.

A verified binary bundle also contains `demo`. From its extracted root,
`./demo --assert` starts a disposable server, writes and queries the bundled
budget example through public binaries, and shuts down with SIGTERM. It uses no
source-tree Cargo command or test harness.

The complete acceptance demonstration is:

```bash
./scripts/demo --assert
```

It requires either `RIFFDB_BUDGET_POSTGRES_URL` pointing at a dedicated
PostgreSQL 18.4 database or Docker for an ephemeral digest-pinned PostgreSQL
container. The script is fail-closed: unavailable live evidence, a skipped
recovery matrix, a mismatched fixture, or an unmet engine-comparison dependency
is an error.

## Operations

For agent-facing value formats, retries, discovery, and command resources, see
the [MCP agent cookbook](docs/mcp/agent-cookbook.md).

- [Installation and systemd](docs/installation.md)
- [Configuration reference](docs/configuration.md)
- [Offline backup and restore](docs/backup-restore.md)
- [Upgrade and removal](docs/upgrade-removal.md)
- [Security posture and threat model](docs/security.md)
- [Compatibility](docs/compatibility.md)
- [Known limitations](docs/known-limitations.md)
- [Release verification](docs/release.md)

Destructive restore has an important POC limitation: it preserves `DatabaseId`
but rewinds history without an incarnation fence. Sequences in the destroyed
suffix can later be reused, and every locator, cursor, idempotency assumption,
and authorization fact from that suffix must be discarded. Read
[Offline Backup and Restore](docs/backup-restore.md) before operating it.

## License

Copyright © 2026 Kevin O'Shea and O'Shea & Sons, LLC.

RiffDB is an open-source project of
[O'Shea & Sons, LLC](https://osheaandsons.com/).

RiffDB is licensed under either the MIT License or the Apache License, Version
2.0, at your option. See `COPYRIGHT`, `LICENSE-MIT`, and `LICENSE-APACHE`.
