---
adr: "0304"
title: Offline SDK Generation Role Coverage Diagnostics And Dev Loop Ergonomics
status: accepted
tier: surface
date: 2026-10-14
accepted: '2026-10-14'
acceptance: 'maintainer, in session, 2026-10-14: "Yes, register and build", in response to the DevEx ADR-0303/ADR-0304 and WP-835..WP-841 proposal'
requires: [ADR-0041, ADR-0121, ADR-0122, ADR-0155]
amends: [ADR-0041, ADR-0121, ADR-0122]
supersedes: []
requirements: [DX-004, DX-005, DX-012, DX-014, DX-016, DX-017, DX-021, DX-025, DX-046]
packages: [WP-838, WP-839, WP-840, WP-841]
obligations: []
review_triggers:
  - Offline lock synthesis weakens parent-aware successor verification during riffdb push or riffdb application deploy.
  - Role-coverage diagnostics expose secret values or permit unauthorized operations to execute at runtime.
  - riffdb dev --reset deletes files outside the repository-confined .riffdb/dev directory.
---
# ADR-0304: Offline SDK Generation Role Coverage Diagnostics And Dev Loop Ergonomics

## Context

Startup teams and coding agents iterating on RiffDB applications encounter four CLI and authoring workflow bottlenecks:

1. Running `riffdb init` followed by `riffdb generate` fails when `riffdb.application.lock.json` is absent or stale, because lock materialization currently requires `riffdb push` against a running server. Conversely, `riffdb push` updates the lock without regenerating the local SDK bindings unless the user runs a second command.
2. When an author or coding agent adds a `command` to `.riff` or a `query` to `.riffql` and forgets to list it under a role in `riffdb.application.json`, `riffdb check` and `riffdb generate` succeed silently while omitting the operation from every generated SDK client.
3. During rapid MVP prototyping, breaking schema edits fail against existing local `.riffdb/dev` state with forward-only lineage errors unless the developer manually locates and deletes `.riffdb/dev`, and `riffdb dev` refuses workspaces whose `riffdb.toml` or manifest declares more than one generated language surface.
4. The embedded agent skill (`crates/riffdb-cli/assets/agent/SKILL.md`) installed by `riffdb agent init` is a 35-line command list with no `.riff`, `.riffql`, `riffdb.application.json`, or generated SDK examples, forcing coding agents to guess syntax.

## Decision

1. **Offline lock materialization in `riffdb generate` and `--generate` flag on `riffdb push` (`WP-838`):**
   - When `riffdb generate` runs in a project whose `riffdb.application.lock.json` is missing or out of date with the local symbolic source, it compiles the local `.riff`, `.riffql`, and `riffdb.application.json` artifacts deterministically in memory, writes the updated compiler-owned `riffdb.application.lock.json` (using genesis compilation when no parent lock exists, or explicit local lock refresh), and materializes the configured SDK surfaces without requiring a running server.
   - `riffdb push` gains a `--generate` flag (and default project-mode regeneration when `[project].generators` is non-empty) so pushing a schema and refreshing local SDK bindings complete in one step. Remote parent-aware successor verification during `riffdb push` and `riffdb application deploy` remains unchanged.
2. **Manifest role-coverage diagnostics (`WP-839`):**
   - Source checking (`riffdb check`, `riffdb generate`, `riffdb dev`, and `riffdb lsp`) compares the compiled contract command catalog and query-module catalog against the union of operations granted across all roles in `riffdb.application.json`.
   - Any compiled command or query not granted to at least one declared role in a non-empty role manifest produces a structured diagnostic naming the unreferenced operation and the `riffdb.application.json` path with an actionable fix hint.
3. **Ephemeral `riffdb dev --reset` and multi-surface `riffdb dev` regeneration (`WP-840`):**
   - `riffdb dev` gains a `--reset` flag that safely removes the workspace-confined `.riffdb/dev` state directory before bootstrapping the local server and regenerating bindings. Path validation refuses symlinks, parent traversal, or paths outside `<project_root>/.riffdb/dev`.
   - `riffdb dev` materializes all declared and selected generated SDK surfaces in a polyglot workspace (for example, TypeScript plus Python) during initial bootstrap and watch-loop reloads; `--run` continues to require a single selected runner target or explicit target selector.
4. **Self-contained `riffdb agent init` skill and compiler fix hints (`WP-841`):**
   - Expand `crates/riffdb-cli/assets/agent/SKILL.md` into a self-contained authoring reference covering `.riff` entities and commands, `.riffql` projections and queries, `riffdb.application.json` role grants, and idiomatic generated SDK usage (one-line `connect`, outcome helpers, `.causal()` read-your-writes, and pagination) across TypeScript, Python, Rust, and Go while preserving all `DX-025` markers.
   - Enrich common `.riff` and `.riffql` syntax/lowering diagnostics with concise fix hints for frequent agent authoring mistakes.

## Consequences

- `riffdb init && riffdb generate` works completely offline, `riffdb check` catches forgotten role grants immediately, `riffdb dev --reset` recovers cleanly from destructive prototype schema edits, and `riffdb agent init` gives coding agents a complete, accurate reference in one file.

## Standing design tests

- **Interface safety:** Offline lock compilation never bypasses server-side parent-lineage checks on `riffdb push` or `deploy`; role-coverage diagnostics never auto-grant authority; and `riffdb dev --reset` is strictly confined to `<workspace>/.riffdb/dev`.
- **Scale:** Role-coverage checking is a bounded $O(C + Q)$ set membership pass over compiled catalog identifiers at compile time; `SKILL.md` remains a static embedded asset under existing size bounds.

## Checks

- CLI integration tests for offline `riffdb init` + `riffdb generate`, `riffdb push --generate`, unreferenced command/query role-coverage diagnostics, `riffdb dev --reset` state wipe and symlink refusal, polyglot `riffdb dev` surface regeneration, and `DX-025` agent skill parity checks.
