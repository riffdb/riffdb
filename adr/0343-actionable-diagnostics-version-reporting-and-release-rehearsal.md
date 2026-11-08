---
adr: "0343"
title: Actionable Diagnostics Version Reporting And Release Rehearsal
status: proposed
tier: surface
date: 2026-11-08
accepted: null
acceptance: null
requires: [ADR-0041, ADR-0065, ADR-0304, ADR-0329, ADR-0340, ADR-0341, ADR-0342]
amends: [ADR-0041, ADR-0304, ADR-0340]
supersedes: []
requirements: [DX-003, DX-005, DX-006, DX-007, DX-009, DX-012, ID-005, VER-001]
packages: [WP-918, WP-919, WP-920, WP-921, WP-922]
obligations: []
review_triggers:
  - Diagnostics disclose secret values, absolute paths, submitted literals, unauthorized schema, health or database identity, or arbitrary dependency prose.
  - Deployment accepts degraded readiness, bypasses identity checks or provisions broader authority to avoid a startup failure.
  - Machine output changes a closed v1 shape without an explicit version and reviewed compatibility fixtures.
  - Compiler fixes change accepted language semantics, concurrency fences or command lowering rather than diagnostics.
---
# ADR-0343: Actionable Diagnostics Version Reporting And Release Rehearsal

## Context

The released todo/adapter rehearsal required manual conversion of source offsets,
separate health calls, configuration trial-and-error, and packaging/startup
workarounds. Generic configuration errors lose causes, deploy discards authenticated
health components, compiler hints retain obsolete grammar language, and neither
primary binary supports a version flag. ADR-0340 separately owns specific protected
file/TLS errors and deployment ergonomics; this record consumes that work instead
of duplicating its credential or deployment-state machinery.

## Decision

1. Complete all five announcement-readiness tracks: configuration diagnostics,
   deployment readiness diagnostics, compiler diagnostics, operational logging
   plus version reporting, and release-artifact integration rehearsal. Logging,
   tracing and metrics are implemented by ADR-0341/0342 and are dependencies of
   qualification, not substitutes for the other tracks.
2. Extend ADR-0340's existing typed configuration failure classes with setting
   role, effective source (`flag|environment|file|default`), bounded closed cause,
   expected rule and actionable fix. Preserve current precedence and fail before
   network I/O. Cover TOML shape, unknown settings, aliases, retry bounds, path
   roles, TLS identity, permissions and ownership without exposing values,
   absolute paths, raw parser excerpts or internal error sources.
3. Preserve `configuration_invalid` and `database_readiness_unavailable` as top
   codes. Add opt-in `--output json-v2` and `client.output = "json-v2"` /
   `RIFFDB_OUTPUT=json-v2`, selecting envelope `riffdb.cli.output/v2`.
   V2 retains the v1 terminal envelope and result/error bodies and adds a
   bounded sibling `diagnostics` list (at most 32 entries, total at most 64 KiB)
   carrying closed cause/setting/source/component/fix fields where applicable.
   Existing `--output json` stays byte-compatible with the merged WP-913 v1
   baseline; human mode gains the same
   actionable facts. V1 never hides extra JSON objects on stdout. Freeze all
   new DTOs, enum vocabularies, optional-field rules, examples and exit behavior
   in interface fixtures before implementation. Regenerate owned adapters from
   their authoritative generator inputs; never hand-edit generated output.
4. On deploy refusal, consume the existing authenticated health response and
   report every blocking component, safe status/reason and supported repair in
   one bounded diagnostic. Distinguish component degradation, alias mismatch,
   absent audience, pre-bootstrap health and absent evidence. Identify only
   authorized database information; keep untrusted/mismatched identity details
   redacted. Do not send another health RPC or infer causes absent from health.
   Preserve readiness, exact locks, least-authority provisioning and resume state.
5. Compiler human output uses workspace-relative `path:line:column`, with
   one-based lines/Unicode-scalar columns computed from the exact compiled
   source snapshot, LF/CRLF normalization and explicit tab treatment (one scalar
   column). EOF and multiline spans remain valid; byte spans stay authoritative.
   CLI LSP continues zero-based UTF-16; test shared conversions without changing
   that protocol. Do not reread a changed file to reconstruct a location.
6. Authoring JSON version 1 remains readable and byte-compatible when selected.
   Under `--output json-v2`, authoring diagnostics select version 2, preserving
   existing byte spans and adding start/end line/column plus bounded expected
   tokens, safe found-token class and current help. The found token is a closed
   keyword/punctuation/category, never a source literal or arbitrary identifier.
   Retain current diagnostic codes. Cover `.riff`, named query sources and
   existing query suffixes with the same semantics across check/generate/push/
   lock/deploy/dev and existing authoring consumers. MCP diagnostics retain
   their accepted shape until separately selected version support is available.
7. Propagate existing bounded expected-token alternatives for RDB-S004 and
   provide precise grammar-context help. Replace obsolete grammar-version-1
   wording using current authoritative language/version registries; avoid
   manually maintaining a second grammar version. Unsupported `u32` guidance
   names supported bounded types and the need to choose appropriate range;
   never rewrite source or silently broaden accepted values. Preserve and test
   corrected `checked_by` spelling. Freeze source-span snapshots and semantic
   assertions for each changed compiler diagnostic; do not alter guard inference.
8. `riffdb --version` and `riffdbd --version` print exactly one bounded
   `<binary> <workspace-version>` line to stdout, empty stderr, exit 0, without
   config, credential, storage, subscriber, listener or network initialization.
   `-V` is the alias. Parser failures retain existing safe behavior; unknown
   daemon flags and missing values are distinct. Amend ADR-0041's early version
   stderr rule for these flags only. Define these as standalone invocations;
   invalid combinations fail consistently, without acting on other flags.
9. Qualify release archives, container images and driver distributions from one
   exact version/revision in a clean environment without source-build fallbacks.
   Verify anonymous image pull, checksums including driver distribution,
   executable inventory, offline version reporting, and Go/TypeScript/Python/
   Rust SDK smoke coverage. Reuse accepted adapter evidence and pinned inputs.
   Run fresh-install and retained-state restart scenarios for one server/four
   isolated databases with Better Auth, OpenFGA, MLflow and the todo app.
   Exercise signup/signin, CRUD/sharing, organization, stale atomic rejection,
   idempotent retry and authorization isolation. No hidden direct-role-binding
   readiness workaround or dependency on an optional external AI provider.
10. Inject the representative config/readiness/compiler/reload failures and
    assert actionable redacted diagnostics in human/v1/v2 modes as applicable;
    collect JSON logs, correlated traces and metrics with a local collector.
    Publish only sanitized evidence, stable synthetic fixtures and documented
    interface usage, never secrets, machine addresses, runtime directories or
    raw logs. An isolated local packaging pass does not prove published asset
    availability: actual anonymous downloads/pulls are a separate release gate.

## Standing design tests

- **Interface safety:** diagnostic richness cannot bypass authorization,
  redaction, bounds, readiness, locks, idempotency or durable command guarantees.
  V1 remains compatible; V2 is explicit; no raw-source snippets are introduced.
- **Scale:** source maps are linear in existing bounded source bytes; locations,
  diagnostics, health components, rendering and rehearsal waits remain bounded.

## Checks

- Config precedence/canary tests, deploy component/mismatch/refusal tests, compiler
  source-span/location snapshots, and real-binary offline version process tests.
- Explicit v1/v2 compatibility and malformed/oversized diagnostic fixtures.
- Installed-artifact adapter/todo/collector rehearsal, package acceptance,
  handbook/generated-reference checks and full CI before merge/announcement.
