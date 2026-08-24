# WP-362 through WP-391 closure audit

This audit closes the first durability/performance campaign and the
multi-database/first-real-application developer-experience campaign against
current `main`.

## Durability and performance campaign

The implementation receipts are retained in:

- `docs/performance/wp-362-command-growth.md`;
- `wp-364-group-durability.md`;
- `wp-366-write-parity.md`;
- `wp-367-369-command-pipeline.md`;
- `wp-371-semantic-durability-profiles.md`;
- `wp-373-compact-storage-v2.md`;
- `wp-374-authoritative-event-references.md`;
- `wp-375-partition-index-generations.md`;
- `wp-376-timer-free-scheduler.md`;
- `wp-377-safe-read-hot-path.md`;
- `wp-378-command-cpu-pipeline.md`; and
- `wp-379-parity-decision.md`.

This arc removed the quadratic audit append, introduced bounded physical
groups and audited terminal fusion where proved safe, split activated writer
and MVCC read authority, retained immutable compiler artifacts, adopted
semantic one-phase standard durability, compacted the durable format with a
restartable migration, made event payload ownership canonical, bounded index
generation invalidation, removed timer-wheel grouping parks, and reduced
repeated read/command validation work.

The evidence is deliberately not rewritten:

- WP-366's first write comparison was later found to contain replay-shaped
  samples; its safety implementation remains valid, and later corrected
  app-baseline evidence supersedes the invalid table.
- WP-369's strict all-operation parity gate remained red after its implementation.
- WP-379 published an immutable `not eligible` decision. It is closed as an
  honest failed gate, not a release pass.
- Campaign 02 subsequently passed after further repaired public-only packaging
  and application work; `docs/agent-application-alpha.md` retains that result.

Current performance qualification is owned exclusively by WP-623/WP-674.

## Multi-database and first-real-application campaign

WP-380 through WP-391 shipped:

- exact multi-database and MCP compatibility rules;
- database selectors through CLI, clients, MCP, health, and active identity;
- isolated per-database server composition and operator lifecycle;
- installed add-database/bootstrap/deploy/provision/seed workflows;
- fail-closed sibling backup-root validation and actionable startup errors;
- schema-directed CLI/MCP JSON values and symbolic field diagnostics;
- exact lock/module/role identity reconciliation before mutation;
- application command/query MCP tools under the bound application credential;
  and
- the public first-real-application authoring reference and troubleshooting
  guidance.

The successor evolution and installed-app cohort (WP-392 onward) is already
closed separately. Current package-first multi-language campaigns exercise the
same public configuration and application identities.

## Current-head verification and release boundary

Repository-wide all-feature tests, focused storage/recovery/migration tests,
application package acceptance, binding/boundary checks, generated artifacts,
formatting, Clippy, dependency policy, requirement coverage, and handbook
checks passed in the closure campaign. This records implementation and current
regression coverage only. WP-578, WP-579, WP-623, and WP-674 remain open.
