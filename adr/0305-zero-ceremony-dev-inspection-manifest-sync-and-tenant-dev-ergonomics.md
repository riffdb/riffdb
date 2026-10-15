---
adr: "0305"
title: Zero Ceremony Dev Inspection Manifest Sync And Tenant Dev Ergonomics
status: accepted
tier: surface
date: 2026-10-15
accepted: '2026-10-15'
acceptance: 'maintainer, in session, 2026-10-15: "i accept, please build", in response to the ADR-0305 and WP-842..WP-844 proposal'
requires: [ADR-0015, ADR-0041, ADR-0121, ADR-0155, ADR-0303, ADR-0304]
amends: [ADR-0015, ADR-0041, ADR-0121, ADR-0304]
supersedes: []
requirements: [DX-004, DX-005, DX-014, DX-016, DX-017, DX-046, DRV-005, DRV-007, DRV-008]
packages: [WP-842, WP-843, WP-844]
obligations: []
review_triggers:
  - Inline or dev-session CLI inspection allows mutating operations, unbounded scans, or bypasses role/bearer authorization.
  - Manifest query or role synchronization silently grants newly authored commands or queries in a multi-role manifest without an explicit --sync-role target.
  - Tenant-scoped dev role binding weakens partition/tenant isolation or permits tenant-scoped roles to execute without an explicit tenant ID.
---
# ADR-0305: Zero Ceremony Dev Inspection Manifest Sync And Tenant Dev Ergonomics

## Context

Even after `ADR-0303` and `ADR-0304` streamlined generated SDK ergonomics and offline lock synthesis, three high-frequency developer and coding-agent workflows still require multi-file or multi-flag ceremony during MVP iteration:

1. **Ad-hoc read inspection during debugging:** Inspecting live state while `riffdb dev` is running requires manually copying the ephemeral dev endpoint, bearer credential path, contract lineage, and contract version into `riffdb entity get` or authoring a `.riffq` file plus a separate `--parameters <JSON_PATH>` file for `riffdb query run`. Coding agents debugging a failed test cannot run a single one-liner read query against the local dev instance.
2. **Multi-file synchronization on every new command or query:** Adding a command or query requires editing the `.riff` or `.riffq` file, registering any new `.riffq` path in `riffdb.application.json` under `queries`, and adding the operation name to `roles[0].commands` or `roles[0].queries` before locking and regenerating bindings. While `WP-839` catches omissions with `RDB-AR011`, single-role MVP projects still force a manual three-file edit on every iteration.
3. **Multi-tenant role testing in `riffdb dev`:** Startup MVPs frequently model B2B multi-tenant roles (`"tenant_scope": "tenant"`), and `compile_application_role_v2` plus `riffdb role bind --tenant <TENANT>` already support tenant-scoped roles. However, `scripts/riffdb-dev` explicitly rejects any role whose `tenant_scope` is not `"global"`, blocking local `riffdb dev` workflows for multi-tenant applications.

## Decision

1. **Zero-ceremony ad-hoc dev query and entity inspection (`WP-842`):**
   - When `riffdb dev` (`scripts/riffdb-dev`) is running, it writes a workspace-confined mode-`0600` `.riffdb/dev/session.json` file containing the non-secret dev endpoint, database alias, active contract lineage/version, bound role, optional tenant ID, and workspace-confined credential file path, and removes it on exit.
   - When `riffdb query run`, `riffdb query run-named`, or `riffdb entity get` is invoked inside a project workspace without explicit `--endpoint`, `--credential-file`, or `--contract-lineage`/`--contract-version` flags, the CLI resolves missing defaults from `.riffdb/dev/session.json` (falling back to `riffdb.application.json` / `riffdb.application.lock.json` for contract lineage and version).
   - `riffdb query run` gains `--inline <RIFFQL>` (mutually exclusive with a file path) and repeatable `--param <KEY=VALUE>` plus `--params-json <JSON_OBJECT>` flags (with `--parameters <PATH>` optional and defaulting to `{}`) so developers and agents can run bounded, read-only ad-hoc RiffQL queries in one command without mutating `riffdb.application.lock.json`.
2. **Opt-in manifest query discovery and single-role operation sync (`WP-843`):**
   - `riffdb application check`, `riffdb application lock`, `riffdb generate`, and `riffdb dev` gain `--sync-manifest` and `--sync-role <ROLE>`.
   - When `--sync-manifest` (or `--sync-role <ROLE>`) is passed, the CLI deterministically discovers `*.riffq` files under the project's `riffdb/queries/` directory, registers missing query module paths in `riffdb.application.json` in canonical lexical order, and grants newly compiled commands and named queries to the target role in canonical sorted order before compiling the lock and regenerating SDKs.
   - If `riffdb.application.json` declares exactly one role, `--sync-manifest` targets that role automatically. If the manifest declares multiple roles and `--sync-role <ROLE>` is not specified, `--sync-manifest` fails closed with an actionable diagnostic rather than guessing which role should receive authority.
3. **First-class tenant-scoped roles in `riffdb dev` and generated SDKs (`WP-844`):**
   - `riffdb dev` (`crates/riffdb-cli` and `scripts/riffdb-dev`) gains `--tenant <TENANT_ID>`. When the selected role in `riffdb.application.json` declares `"tenant_scope": "tenant"`, `riffdb dev` binds the role with `--tenant <TENANT_ID>` (defaulting to `"dev-tenant"` in local dev when `--tenant` is omitted) instead of rejecting non-global roles, and exports `RIFFDB_TENANT=<TENANT_ID>` to child runners and `.riffdb/dev/session.json`.
   - Generated SDK clients across Rust, TypeScript, Python, and Go expose the optional bound `tenant` / `tenant_id` from connection options or `RIFFDB_TENANT` on the client configuration for inspection and diagnostics while preserving server-enforced bearer-token tenant scoping.

## Consequences

- Developers and coding agents can inspect entities and run bounded one-liner RiffQL queries against a running `riffdb dev` session with zero flag ceremony, sync single-role manifests in one flag during schema iteration, and use `riffdb dev` with `"tenant_scope": "tenant"` roles out of the box.

## Standing design tests

- **Interface safety:** Inline queries execute through the unchanged bounded RiffQL compiler and read-only query execution path (`read_only = true`, mandatory bounds/ordering, and bearer authorization) and never mutate `riffdb.application.lock.json`. `--sync-manifest` is explicit and refuses ambiguous multi-role manifests without `--sync-role`. Tenant binding remains cryptographically bound to the role bearer credential on the server.
- **Scale:** `.riffdb/dev/session.json` is a small bounded JSON file (`<= 4 KiB`); query directory discovery scans only `<project_root>/riffdb/queries/*.riffq` in canonical lexical order.

## Checks

- CLI unit and integration tests for `.riffdb/dev/session.json` auto-discovery, `riffdb query run --inline` with `--param` / `--params-json`, `--sync-manifest` single-role sync and multi-role refusal, and `riffdb dev --tenant` with tenant-scoped roles and generated SDK `RIFFDB_TENANT` propagation.
