---
adr: "0329"
title: Developer Onboarding CLI And Handbook Restructuring
status: accepted
tier: surface
date: 2026-11-04
accepted: '2026-11-04'
acceptance: 'maintainer, in session, 2026-11-04: "yes, this sounds good. use a worktree, and follow all the repo rules around creating work-packages etc", in response to the developer onboarding CLI and documentation restructuring plan'
requires: [ADR-0041, ADR-0121, ADR-0304, ADR-0305, ADR-0308]
amends: [ADR-0041, ADR-0121, ADR-0308]
supersedes: []
requirements: [DX-004, DX-005, DX-013, DX-014, DX-015, DX-016, DX-017, DX-046]
packages: [WP-881, WP-882]
obligations: []
review_triggers:
  - riffdb init without --starter alters the domain-empty contract or zero-role manifest required by DX-013 through DX-015.
  - Standalone riffdb dev bypasses capability bootstrap, exact lock verification, or role-bound credential scoping, or leaves orphan server processes on exit.
  - Handbook restructuring removes any public handbook page from docs/SUMMARY.md or weakens adapter, Helm, or driver-registry evidence checks.
---
# ADR-0329: Developer Onboarding CLI And Handbook Restructuring

## Context

First-time developers evaluating RiffDB encounter two onboarding barriers across the CLI and public handbook:

1. **CLI first-run friction outside a source checkout:** `riffdb init` creates a domain-empty contract (`contract <Name> version 1 {}`) with zero entities, commands, queries, or roles (`DX-013`), so running `riffdb generate` or `riffdb dev` immediately after `riffdb init` produces an empty client with no callable operations until the developer manually authors three files. Furthermore, `riffdb dev` delegates to an external bash script (`riffdb-dev`) that is absent when `riffdb` and `riffdbd` are installed from the three-binary release archive (`riffdb-<version>-<target>.tar.gz`) or `cargo riffdb install`.
2. **Overwhelming installation and getting-started documentation:** `docs/installation.md` is a 765-line operator manual mixing first-time developer setup with multi-database `--add-database` migration journals, manual `openssl rand` digest key scripts, manual `mapfile` bootstrap recovery loops, and a 50-line manual database reset procedure. Meanwhile, `# Start Here` in `docs/SUMMARY.md` lists 11 competing pages without a single step-by-step tutorial showing how to author a contract, write a RiffQL query, generate a typed client, and call it from application code.

## Decision

1. **Opt-in starter template for `riffdb init --starter` (`WP-881`):**
   - `riffdb init` gains a `--starter` flag (`crates/riffdb-cli/src/cli.rs`, `crates/riffdb-cli/src/project.rs`, `crates/riffdb-cli/src/scaffold.rs`).
   - When `--starter` is omitted, `riffdb init` preserves the exact `DX-013`/`DX-014`/`DX-015` domain-empty contract and zero-role manifest.
   - When `--starter` is passed, `riffdb init` idempotently writes `riffdb.toml`, `riffdb.application.json` (declaring the selected generators plus a `<PascalName>Application` role granting `CreateItem` and `ItemPage`), `riffdb/contract.riff` (`Item` entity, `ItemRoot` aggregate, and `CreateItem` command), and `riffdb/queries/item_page.riffq` (`ItemPage` query) without touching existing project files (`package.json`, `pyproject.toml`, `Cargo.toml`, `go.mod`, `src/`), enabling immediate offline `riffdb generate` and `riffdb dev` execution.
2. **Self-contained standalone `riffdb dev` supervisor fallback (`WP-881`):**
   - When `resolve_dev_script` finds an installed or source-tree `riffdb-dev` script, `riffdb dev` delegates to it unchanged.
   - When no `riffdb-dev` script is present on disk, `riffdb dev` runs a native Rust supervisor in `crates/riffdb-cli` that locates `riffdbd` (via `RIFFDB_SERVER_BIN`, sibling executable directory, or `PATH`), compiles and locks the workspace application, creates a mode-`0700` ephemeral runtime directory with fresh capability and idempotency digest keys, spawns `riffdbd` on an ephemeral loopback port, bootstraps an operator capability, deploys the locked contract, query modules, role credential, and optional `--seed` inputs, writes `.riffdb/dev/session.json` and `.riffdb/dev/profile.credential`, emits `riffdb-dev-context-v1` and `riffdb-dev-ready-v1`, executes `--exec` or `--run` (or waits for termination), and reaps `riffdbd` and removes ephemeral state on exit.
3. **Streamlined installation guide, production deployment runbook, and scratch application tutorial (`WP-882`):**
   - Restructure `docs/installation.md` into a concise developer installation guide covering prebuilt release archives, Docker (`ghcr.io/riffdb/riffdb`), ecosystem CLI/driver packages, and `cargo riffdb install --user` + `cargo riffdb bootstrap --user`.
   - Move production `--system` systemd administration, manual digest-key provisioning, manual `capability bootstrap` recovery scripts, multi-database `--add-database` migration journals, and offline database reset procedures to `docs/operations/PRODUCTION-DEPLOYMENT.md`.
   - Rewrite `docs/getting-started/FIRST-APPLICATION.md` into a complete, step-by-step tutorial that walks through initializing a project, authoring an entity and commands (`Task`, `CreateTask`, `CompleteTask`), writing a RiffQL query (`ListTasks`), generating typed SDK bindings (`riffdb generate`), calling them from TypeScript, Python, Go, and Rust against `riffdb dev`, and evolving the contract.
   - Streamline `# Start Here` in `docs/SUMMARY.md` and `README.md` while keeping all public handbook pages reachable and all repository checks passing.

## Standing design tests

- **Interface safety:** `riffdb init --starter` and the standalone `riffdb dev` supervisor use the unchanged compiler, exact application lock, capability bootstrap, and role-bound credential paths; no authority, transport, or storage check is bypassed.
- **Scale:** `riffdb init --starter` preflights at most 4 bounded project files; the standalone `riffdb dev` supervisor bounds child readiness waits, port probes, and teardown deadlines and removes its temporary directory on exit.

## Checks

- Unit and process-level integration tests in `crates/riffdb-cli` (`init_starter_scaffolds_contract_query_and_role_and_generates_offline` and `standalone_dev_supervisor_boots_ephemeral_server_and_executes_command`).
- `./scripts/generate-cli-reference --check` and `./scripts/handbook check`.
