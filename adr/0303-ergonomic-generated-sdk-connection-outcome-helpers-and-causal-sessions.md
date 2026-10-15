---
adr: "0303"
title: Ergonomic Generated SDK Connection Outcome Helpers And Causal Sessions
status: accepted
tier: surface
date: 2026-10-14
accepted: '2026-10-14'
acceptance: 'maintainer, in session, 2026-10-14: "Yes, register and build", in response to the DevEx ADR-0303/ADR-0304 and WP-835..WP-841 proposal'
requires: [ADR-0015, ADR-0037, ADR-0074, ADR-0106, ADR-0148, ADR-0155]
amends: [ADR-0015, ADR-0106]
supersedes: []
requirements: [DRV-001, DRV-005, DRV-007, DRV-008, DRV-012, DRV-013, DRV-016]
packages: [WP-835, WP-836, WP-837]
obligations: []
review_triggers:
  - A generated binding performs remote transport trust, retry, idempotency, or read-after-commit logic outside the Rust driver core.
  - A generated connection factory, outcome helper, causal session wrapper, or pagination helper allows omitting idempotency keys, role credentials, or bounded page limits.
  - Existing explicit transport constructors or result fields are removed rather than preserved alongside the ergonomic facade helpers.
---
# ADR-0303: Ergonomic Generated SDK Connection Outcome Helpers And Causal Sessions

## Context

Generated application bindings for TypeScript, Python, Rust, and Go currently require significant boilerplate for everyday application code compared to modern ORMs:

1. Connecting a generated TypeScript or Go role client requires passing seven compile-time lock and catalog hashes (`applicationManifestHash`, `operationCatalogHash`, `roleDefinitionHash`, `remoteIdentityHash`, `contractLineage`, `contractVersion`, `contractBundleHash`) at runtime and wiring multiple transport wrapper layers (`DriverApplicationTransport` and `DriverGeneratedApplicationTransport`) plus an explicit attempt budget. Connecting a generated Python or Rust client requires importing six to nine low-level credential, metadata, and transport types.
2. Accessing command and query outcomes requires repetitive unwrapping: TypeScript command outcomes sit behind `command.outcome.outcome` and `-> one` queries require hand-written `asserts` type guards; generated Rust query parameter and row structs emit contract enum fields as raw `String` instead of the generated Rust enum type; and generated Python methods require importing and instantiating `*Input` and `*Params` dataclasses for every call rather than also accepting keyword arguments.
3. Causal read-your-own-writes across a request handler requires manually threading `{ readAfterCommit: command.commitSequence }` into every subsequent query call, and iterating over cursor-paginated `-> many` queries requires hand-written `nextCursor` loops.

All four friction points sit entirely inside the compiler-emitted target-language facade and scaffold templates. The compile-time lock identity is already known when `riffdb generate` renders the role client.

## Decision

1. **Embedded role lock identity and one-line connection factories (`WP-835`):**
   - Each generated role client embeds its compiled role name and lock/catalog identity constants (`applicationManifestHash`, `operationCatalogHash`, `roleDefinitionHash`, `contractLineage`, `contractVersion`, `contractBundleHash`) directly in the generated module.
   - Each generated role client exposes a high-level `connect` / `connect_env` factory that defaults `AttemptBudget` to `3`, resolves unspecified endpoint, socket path, database alias, and bearer credential parameters from standard `RIFFDB_*` environment variables, and constructs the underlying Rust-owned driver transport in one call. Existing explicit constructors remain supported unchanged.
   - Application scaffold templates (`riffdb init` / `riffdb new`) use the one-line connection factory.
2. **Ergonomic call signatures, typed Rust query enums, and result/outcome helpers (`WP-836`):**
   - Generated Rust query parameter structs and row structs use the generated Rust enum type (rather than `String`) whenever a parameter or projected column resolves to a declared contract enum, converting to and from `ApplicationValue::Enum` at the generated marshalling boundary.
   - Generated Python sync and async role clients accept keyword arguments (`**kwargs`) alongside explicit `*Input` and `*Params` dataclass instances on command and query methods.
   - Generated command results expose a direct outcome discriminant accessor (`outcomeTag` in TypeScript/Go, `outcome_tag` in Python/Rust) alongside `outcome`.
   - Generated `-> one` query results expose `.unwrapFound()` / `.unwrap_found()` and `.orNull()` / `.or_none()` helpers that return the found row directly (or `null`/`None` when `NotFound`), and generated `-> many` query results expose a direct `.items` accessor alongside `.value.items`.
3. **Request-scoped causal session handles and bounded page iterators (`WP-837`):**
   - Generated command results expose `.readAfterOptions()` / `.read_after_options()` returning query options pre-populated with `readAfterCommit = commitSequence`.
   - Generated role clients expose a lightweight request-scoped `.causal()` handle that tracks the maximum `commitSequence` observed from commands executed through that handle and supplies it as the default `readAfterCommit` floor on subsequent queries unless the caller passes an explicit floor. No global or cross-handle state is mutated.
   - For cursor-eligible `-> many` queries, generated role clients emit typed page and item iterator helpers (`<query>Pages` and `<query>Items` in TypeScript, `<query>_pages` and `<query>_items` in Python/Rust, and page iterator helpers in Go) that advance `nextCursor` through the existing driver continuation path while preserving bounded per-page limits.

## Consequences

- Connecting, executing commands, unwrapping `-> one` and `-> many` results, reading own writes, and paging through results require minimal application code across TypeScript, Python, Rust, and Go.
- Generated facade files grow by a small constant set of helper methods per role client and operation; existing explicit constructors and result properties remain backward-compatible.

## Standing design tests

- **Interface safety:** Every helper delegates to the unchanged Rust-owned driver protocol core and application service. Idempotency keys on commands, bearer credentials, role identity verification, bounded page limits, and explicit freshness semantics remain mandatory; no helper can bypass validation or silently downgrade consistency.
- **Scale:** Embedded identity constants are static per generated role. A `.causal()` wrapper holds one scalar `u64` high-water mark per handle, and generated page iterators fetch one bounded page per step on demand without buffering unbounded result sets in memory.

## Checks

- Unit and golden generator tests across Rust, Go, TypeScript, and Python asserting embedded lock constants, one-line `connect` factories, typed Rust query enums, Python keyword-argument calls, `.unwrapFound()` / `.orNull()` / `.items` / `.outcomeTag` helpers, `.causal()` high-water mark propagation, and cursor pagination iterators.
- Updated scaffold templates and `./scripts/check-generated` verification.
