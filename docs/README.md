# RiffDB Handbook

RiffDB is a standalone, contract-first operational database. Applications
declare entities, commands, invariants, outcomes, queries, and least-authority
roles. They mutate state only by invoking compiled commands; RiffDB owns the
concurrency checks, idempotency record, durable outcome, provenance, events,
and projection progress.

RiffDB is currently a **single-node proof of concept**. It is suitable for
evaluation and local agent-built applications, not production workloads. Read
[Known Limitations](known-limitations.md), [Compatibility](compatibility.md),
and [Security](security.md) before choosing an environment.

## Choose a path

| Goal | Start here |
|---|---|
| Understand the model | [What RiffDB Is](getting-started/WHAT-IS-RIFFDB.md) |
| Install a user service | [Installation](installation.md) |
| Build and run an application | [Your First Application](getting-started/FIRST-APPLICATION.md) |
| Give an agent typed database tools | [MCP Agent Cookbook](mcp/agent-cookbook.md) |
| Compare the safety model with PostgreSQL | [PostgreSQL Safety Comparison](tutorials/POSTGRESQL-COMPARISON.md) |
| Operate multiple local databases | [Multiple Databases](operations/MULTIPLE-DATABASES.md) |
| Look up a command | [CLI Reference](reference/CLI.md) |

## The application boundary

```text
contract + RiffQL + symbolic roles
                 |
                 v
       checked application lock
                 |
       +---------+---------+
       |         |         |
     Rust    TypeScript   Python       MCP
       |         |         |            |
       +---------+---------+------------+
                 |
       shared application service
                 |
 deterministic runtime + commit coordinator
                 |
          authoritative storage
```

The transports do not define separate semantics. CLI, generated clients, gRPC,
and MCP all enter the same application service and authorization boundary. MCP
never opens storage directly.

## First successful loop

From a source checkout:

```bash
cargo run -p riffdb-cli -- new item-desk
cd item-desk
../target/debug/riffdb dev --seed --run
```

The scaffold contains the contract, a bounded query, seed commands, exact lock,
and generated clients. Continue with [Your First
Application](getting-started/FIRST-APPLICATION.md) for the review boundaries and
installed workflow.

## What the database proves

- Business invariants are evaluated by the database under concurrency.
- A retry keeps one idempotency identity and recovers the committed outcome.
- A commit atomically records mutations, outcome, events, provenance, and its
  commit-log entry.
- Queries execute against one authoritative snapshot with compiler-proven
  bounds.
- Projections expose frontiers so callers can request read-after-commit
  consistency explicitly.
- Credentials expose named operations and scopes, not ambient storage access.
