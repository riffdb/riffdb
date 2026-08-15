# ADR-0121: Empty Project Schema and Selected Binding Materialization

- **Status:** Accepted
- **Direction approved:** 2026-08-14 (maintainer, in session)
- **Exact text accepted:** Yes — the maintainer approved the prerequisite design
  and implementation direction before WP-602 proceeded
- **Supersedes:** No accepted decision
- **Extends:** ADR-0057 and ADR-0120 section 2

## Context

ADR-0120 requires `riffdb init` to add an empty schema to a new or existing
project and requires `riffdb generate` to emit the targets selected by
`riffdb.toml`. The current symbolic application interface requires a nonempty
query module, a nonempty role list, and materializes every language artifact
retained by the compiler-owned application lock. Treating the sample `Item`
domain as empty, or treating configured targets as advisory, would contradict
the accepted developer interface.

The application lock intentionally binds deterministic generated artifacts.
Changing its persistent encoding merely to select which of those already-bound
artifacts are present in one checkout would couple database installation to a
local packaging preference and create unnecessary acceptance ceremonies.

## Decision

1. A domain-empty symbolic application is valid. Its contract may contain no
   domain declarations, its structural query module may contain zero queries,
   and it may declare zero application roles. The structural module retains the
   existing single-module generation boundary and exact module identity; it
   grants no authority and exposes no operation.
2. Empty collections remain explicit, canonically ordered, and bounded. A
   transition from empty to nonempty uses the same compiler, lock, compatibility,
   authorization, deployment, and migration paths as every other application.
3. The compiler-owned lock continues to bind the deterministic artifact for
   every supported language target plus internal MCP, manifest, bundle, and
   migration artifacts. No persistent lock or IR encoding changes.
4. `riffdb.toml` selects a bounded materialization subset of the language
   artifacts already bound by the exact lock. Internal artifacts are always
   materialized. Project check, push, deploy, status, and diff require the lock,
   sources, and internal artifacts to be exact and require every selected local
   language artifact to be exact; an unselected language artifact may be absent.
5. `riffdb generate` writes only selected language artifacts and never deletes an
   unselected artifact. Changing only the selected target set does not change
   database state or compiler-owned identity and therefore requires no push
   acceptance ceremony.
6. Existing `riffdb application ...` behavior is unchanged: its self-contained
   source manifest still materializes and checks the complete artifact set.

## Interface-safety design test

An application developer cannot opt out of contract, query-module, lock,
internal-artifact, authorization, migration, or deployment exactness. Target
selection controls only which already-verified language presentation artifacts
are materialized locally. It cannot introduce handwritten semantics, change a
server operation, suppress MCP artifacts, bypass the lock, or acknowledge a
write. Empty roles grant no capabilities, and empty query modules expose no
query tools.

## Compatibility

The query-module and application-manifest parsers newly accept zero queries in
one bounded structural module and zero roles. Existing canonical documents and
identities are unchanged. Application locks and contract bundles retain their
accepted encodings. The new states require checked compatibility fixtures and
round-trip tests before WP-602 completion.

## Consequences

- `riffdb init` can be truthful and domain-neutral.
- Local target selection does not cause a database deployment or acceptance
  ceremony.
- Project-aware checking needs an explicit selected-materialization API; the
  legacy full-materialization API remains unchanged.
- A project with no roles cannot provision an application credential until the
  author adds an explicit role and pushes its accepted identity.
