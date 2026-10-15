---
adr: "0308"
title: CLI Command Ergonomics Symbolic Entity Get Migrate Up And Dev Exec
status: accepted
tier: surface
date: 2026-10-15
accepted: '2026-10-15'
acceptance: 'maintainer, in session, 2026-10-15: "All of these are great, yes do ot", in response to the ADR-0308 5-item developer experience proposal'
requires: [ADR-0041, ADR-0121, ADR-0304, ADR-0305, ADR-0307]
amends: [ADR-0041, ADR-0121, ADR-0305]
supersedes: []
requirements: [DX-004, DX-005, DX-014, DX-016, DX-017, DX-046]
packages: [WP-852, WP-853, WP-854]
obligations: []
review_triggers:
  - CLI command execution bypasses contract-compiled command dispatch, bearer authorization, or explicit idempotency payload encoding.
  - Symbolic entity get permits unbounded scans instead of exact primary-key lookup or alters wire encoding.
  - One-command schema migration skips server preflight migration check or confirmation hash verification.
  - Ephemeral dev --exec leaks orphan server or driver-host child processes on command exit.
---
# ADR-0308: CLI Command Ergonomics Symbolic Entity Get Migrate Up And Dev Exec

## Context

Following `ADR-0303`, `ADR-0304`, `ADR-0305`, and `ADR-0307`, five day-to-day CLI workflows still require unnecessary ceremony for application developers and coding agents:

1. **Ad-hoc CLI command execution (`riffdb command run`):** Unlike `riffdb query run` (which supports `--param`, `--params-json`, and `.riffdb/dev/session.json` discovery after `ADR-0305`), `riffdb command run` still requires authoring a JSON file on disk for `--input <PATH>`, manually passing `--endpoint` and `--credential-file`, and manually minting a UUIDv7 for the command's `idempotency` field.
2. **Opaque numeric IDs and base64 keys in `riffdb entity get`:** `riffdb entity get` requires a numeric `ENTITY_TYPE_ID` and a canonical base64-encoded binary `--entity-key` plus numeric `--field` IDs, forcing developers to look up internal entity/field numbers and hand-encode binary primary keys to inspect a single record.
3. **Multi-step schema migration ceremony:** Evolving a schema against a running server requires computing a migration plan, extracting the `migration_hash`, minting an `operation_id`, running `migration check`, and then running `migration apply --confirm-apply <HASH>`.
4. **File-only `riffdb query explain` / `riffdb query check`:** `riffdb query explain` and `riffdb query check` require a `.riffq` file path and explicit `--contract-lineage` / `--contract-version` flags even inside a project workspace with an active dev session, and `query explain` does not explicitly badge `ADR-0307` covered-index execution (`covered=true`, `point_reads=0`) vs. entity hydration.
5. **No single-command ephemeral test runner in `riffdb dev`:** Running an arbitrary test command (`pytest`, `npm test`, `go test ./...`, `cargo test`) against an ephemeral local RiffDB instance requires starting `riffdb dev` in a separate terminal or background script and manually propagating environment variables and teardown.

## Decision

1. **Ergonomic CLI command execution and dev-session discovery (`WP-852`):**
   - Extend `.riffdb/dev/session.json` and workspace `riffdb.application.json` auto-discovery (`apply_dev_inspection_defaults`) to `riffdb command run`, `riffdb command outcome`, `riffdb query check`, `riffdb query explain`, and `riffdb query describe`.
   - `riffdb command run` gains repeatable `--arg <KEY=VALUE>` and `--input-json <JSON_OBJECT>` flags (making `--input <PATH>` optional when inline arguments are provided, or defaulting to `{}` for zero-arg commands).
   - When the local application contract declares a single `Uuid`-typed `idempotency` field on the target command and the caller omits that field in `--arg` / `--input-json`, the CLI automatically mints and inserts a fresh UUIDv7 string before encoding the command input record.
2. **Symbolic `riffdb entity get` by entity name and natural primary-key fields (`WP-852`):**
   - `riffdb entity get <ENTITY>` accepts either a numeric `ENTITY_TYPE_ID` with `--entity-key <BASE64>` or a symbolic entity name (for example `Ticket`) resolved against the workspace contract (`riffdb.application.json` -> `schema.riff` or `--contract-source`) together with repeatable `--key <FIELD=VALUE>` or `--key-json <JSON_OBJECT>` and symbolic or numeric `--field <FIELD_NAME_OR_ID>`.
   - The CLI encodes the canonical primary-key bytes using the contract's `PrimaryKeySpec` and maps returned numeric field IDs back to symbolic field names in output when the contract is available.
3. **Inline/named `riffdb query explain` & `check` with index-coverage attribution and one-command `riffdb migrate up` (`WP-853`):**
   - `riffdb query explain` and `riffdb query check` gain `--inline <RIFFQL>` and bare query-name resolution against `<workspace>/riffdb/queries/*.riffq`, plus automatic workspace/session lineage and version defaults.
   - `query explain` plan lines explicitly surface `covered=true|false` alongside static `point_reads` and `k`-way merge fan-in so developers can verify `ADR-0307` covering-index optimizations directly from the CLI.
   - `riffdb migrate` gains an `up` subcommand (`riffdb migrate up`) and `riffdb push` gains `--migrate`, which compute the local migration plan, mint a fresh UUIDv7 `operation_id`, run server `check_migration`, and immediately execute `apply_migration` with `confirm_apply = migration_hash`.
4. **Single-command ephemeral test runner (`riffdb dev --exec "<CMD>"`, `WP-854`):**
   - `riffdb dev` (`crates/riffdb-cli` and `scripts/riffdb-dev`) gains `--exec <COMMAND>`, mutually exclusive with `--watch`, `--run`, and `--acceptance`.
   - `--exec <COMMAND>` boots the ephemeral local RiffDB server (and driver host when applicable), exports `RIFFDB_DEV_ROOT`, `RIFFDB_ENDPOINT`, `RIFFDB_DATABASE`, `RIFFDB_CREDENTIAL_FILE`, `RIFFDB_DRIVER_SOCKET`, and `RIFFDB_TENANT`, executes `<COMMAND>` in the project root, cleanly tears down all child processes on exit, and exits with `<COMMAND>`'s status code.

## Consequences

- Developers and coding agents can invoke commands, inspect entities by symbolic name and primary key, explain inline or named queries with covering-index attribution, apply schema migrations in one step, and run ephemeral integration test suites via `riffdb dev --exec`.

## Standing design tests

- **Interface safety:** All CLI shortcuts compile and execute through the unchanged contract-first command, query, entity-get, and two-step preflight+confirmed migration RPC surfaces with bearer authorization intact.
- **Scale:** Inline CLI arguments and workspace contract lookups remain strictly bounded by existing CLI payload and contract source limits.

## Checks

- CLI unit and integration tests for `riffdb command run --arg` with auto-idempotency, symbolic `riffdb entity get`, `riffdb query explain --inline` covering-index output, `riffdb migrate up` / `riffdb push --migrate`, and `riffdb dev --exec` exit-code propagation and teardown.
