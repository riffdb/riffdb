# ADR-0077: Migration Language, IR, and Application Lock

- **Status:** Accepted
- **Direction approved:** 2026-07-31
- **Exact text accepted:** 2026-07-31
- **Acceptance reference:** Human maintainer exact-text acceptance in the
  implementation session on 2026-07-31
- **Decision deadline:** Before WP-406 changes grammar, IR, or application formats
- **Depends on:** ADR-0076
- **Amends:** ADR-0002, ADR-0011, ADR-0013, ADR-0057, ADR-0075

## Context

Migration behavior must be reproducible from author-owned source and exact
compiler artifacts. Inferring every transform from a schema diff cannot express
required-field values, enum mappings, or key changes. Embedding one-shot logic
in every active contract bundle retains operational code as contract semantics.
Arbitrary Rust, SQL, callbacks, lookups, clocks, and randomness violate the
deterministic and statically visible product boundary.

Applications may also need direct upgrades from several supported parent
releases. The current source manifest V2 and application lock V3 pin one exact
successor bundle but cannot bind parent-specific migration artifacts.

## Proposed Decision

Introduce grammar-version-1 `.riffm` files compiled by the existing contract
syntax/compiler pipeline into canonical `MigrationBundleV1`. A source file
declares one lineage and positive `from`/`to` contract version and contains a
bounded ordered set of these semantic declarations:

- semantic-preserving entity, field, enum-variant, command, event, outcome, or
  projection rename;
- logical retirement of one existing stable identity;
- an entity transform with `set`, `replace`, `require`, and `rekey` clauses;
- exhaustive enum `match` mapping; and
- explicit repartition/aggregate/conflict migration acknowledgement.

Indexes, relationships, unique constraints, invariants, aggregate definitions,
and projection definitions come only from the exact successor contract. The
migration compiler compares parent and successor, derives their required work,
and rejects missing, duplicate, unnecessary, or unrelated migration clauses.

Migration expressions may depend only on canonical literals, the complete old
row, and its old primary-key components. They reuse the checked expression
arena, depth/node/value budgets, arithmetic, comparison, Boolean operators, and
canonical ordering. Grammar v1 additionally owns exhaustive `match` and a
closed conversion registry:

- identity and optional wrapping;
- asserted optional unwrapping;
- checked `i64`/`u64` conversion;
- exact decimal precision/scale conversion with no nonzero discarded digit;
- bounded string, bytes, and list narrowing after an explicit assertion;
- bounded element-wise list conversion;
- canonical UUID to/from lowercase hyphenated string; and
- exhaustive enum-variant mapping.

Changing money currency, non-exact timestamp/date conversion, rounding,
truncation, clamping, fallback-on-error, lookups, scans, aggregation, external
input, time, randomness, filesystem/network access, or host code is rejected.
Every assertion failure aborts preflight for the entire migration.

`MigrationBundleV1` contains the exact lineage, parent/candidate versions and
bundle hashes, migration format/grammar/IR versions, canonical typed step DAG,
expression arenas, validation requirements, resource upper bounds, source
hash, compiler version, and domain-separated migration bundle hash. It contains
no operational time, operator, request, backup, database, or filesystem value.
Canonical serialization orders stable identities and dependency steps, not
source declaration order where order has no semantics.

Application source manifest V3 adds a `migrations` array of at most 32 entries.
Each entry names one workspace-relative `.riffm` source and one retained
workspace-relative canonical parent-bundle artifact. Every entry targets the
manifest's successor contract. A release may therefore provide a direct
migration from each supported exact parent; the server never infers or chains
intermediate upgrades.

Application lock V4 preserves V1 through V3 meanings and pins the successor
bundle, every retained parent bundle, every canonical migration bundle, their
paths and content hashes, and the existing generated artifacts. The lock and
generated checks reject missing, stale, overlapping, wrong-lineage,
wrong-version, wrong-hash, duplicate-parent, or unused migration artifacts.
Genesis and ordinary compatible successors need no migration entry.

`riffdb migration plan --application <path>` is a local, read-only lock check.
It selects no database and performs no deployment. It reports exact supported
parent identities, migration hashes, stable step categories, resource bounds,
and source-spanned diagnostics without row values.

The fixed initial bounds are: 1 MiB per source, 32 parent entries, 4,096 typed
steps, the existing expression depth of 32 and global node ceiling, 16 MiB per
canonical migration bundle, and the existing 4 MiB application lock ceiling
because locks contain artifact hashes rather than artifact bytes.

## Options Considered

1. **Embed migrations in `.riff`:** Rejected because one-shot operational logic
   would remain in active contract bundles and complicate bundle semantics.
2. **Infer-only JSON plan:** Rejected because it cannot express application
   backfills or key mappings safely.
3. **Rust/SQL callbacks:** Rejected because dependencies and determinism become
   invisible and a generic mutation path appears.
4. **Separate bounded `.riffm` plus exact lock artifacts:** Proposed because it
   preserves source spans, compiler ownership, reproducibility, and direct
   parent support.

## Consequences

- Contract and migration grammars remain separate but reuse syntax, types, and
  evaluation semantics.
- Application manifests and locks gain new versions and compatibility fixtures.
- Supporting another transform primitive requires a reviewed grammar/IR change,
  not an undocumented executor helper.
- Authors must retain supported parent bundles in the application release.

## Compatibility

V1/V2 application source and V1/V2/V3 lock bytes retain exact decoding and
meaning. `.riffm` and `MigrationBundleV1` are new formats. Existing contract IR
nodes and hashes do not change; migration IR uses separate format and hash
domains. Unknown migration versions or step tags fail closed.

## Security

Compilation is local and authority-free. Source, artifact, and diagnostic bounds
apply before allocation or hashing. Expressions cannot inspect credentials,
operator metadata, other rows, or hidden schema. Public diagnostics contain
symbolic paths and static codes, never submitted row values.

## Testing

- Parser/formatter/span snapshots and malformed-source fuzzing.
- Canonical migration bundle and application source/lock goldens.
- Hash-domain, order-independence, wrong-parent, collision, bound, and strict
  decoder tests.
- Property tests evaluate expressions repeatedly and across page boundaries for
  byte-identical results.
- Generated-artifact checks prove locks and bundles reproduce from source.

## Requirements and Work Packages

- **Requirements:** `MIG-001` through `MIG-005`, `MIG-017`, `MIG-019`,
  `MIG-020`
- **Defines or blocks:** `WP-406`, `WP-407`, `WP-411`, `WP-412`
- **Final evidence:** `WP-413`

## Decision Deadline

Exact human acceptance is required before WP-406 changes contract syntax,
compatibility IR, application source manifests, application locks, or generated
fixtures.
