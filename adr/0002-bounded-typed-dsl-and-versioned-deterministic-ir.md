# ADR-0002: Bounded Typed DSL and Versioned Deterministic IR

- **Status:** Proposed
- **Direction approved:** 2026-07-12
- **Exact text accepted:** No
- **Decision deadline:** Grammar before WP-030 implementation; full record before WP-040 interfaces merge

The human architecture review approved this direction. This record remains
Proposed until its exact text is reviewed and accepted.

## Context

Before the 2026-07-12 governance reconciliation, the specification contained two
incompatible budget-contract sketches. That mismatch demonstrated why parser,
compiler, catalog, runtime, schema generation, and compatibility work require one
bounded language and one versioned executable form. Wall-clock bundle metadata
would also make equal inputs compile to different artifacts.

## Proposed Decision

SPEC Section 7.2 is the canonical v0.1 grammar. Section 23.1 is illustrative
pseudocode and must be reconciled to Section 7.2 before it is an acceptance
fixture. V0.1 does not add modules, scalar aliases, explicit outcome blocks, or a
second idempotency syntax merely to parse the appendix example.

Source parses into a source-oriented AST with spans, then a typed HIR, then a
validated versioned executable IR. The AST is never executable or durable.
Compiler-assigned IDs are stable within contract lineage and are not reused.
Every executable plan explicitly declares bounded reads, writes, conflict keys,
invariants, effects, outcomes, and locality.

`ContractBundle` canonical content includes source/compiler/IR versions and
canonical source and plan hashes. It does not contain `generated_at`. Compilation
and deployment timestamps belong in non-canonical catalog audit metadata.
Identical canonical source, compiler version, and compilation options produce
identical bundle bytes and plan hashes.

## Options Considered

1. **Section 7.2 grammar:** Bounded and consistent with the normative model; the
   approved choice.
2. **Section 23.1 grammar:** Richer surface but silently expands v0.1.
3. **Union of both syntaxes:** Adds aliases and ambiguity without semantic value.
4. **Execute AST:** Avoids an IR but loses validation, compatibility, and stable
   deterministic execution boundaries.

## Consequences

- The appendix budget example must be rewritten as a valid Section 7.2 fixture.
- Unsupported constructs fail with bounded, source-spanned diagnostics.
- Catalogs retain immutable historical bundles needed by admitted work.
- New language constructs or incompatible IR changes require compatibility review.

## Compatibility

Grammar version, stable IDs, IR version, canonical bundle bytes, and plan hashes
are explicit boundaries. Catalog audit metadata may evolve independently because
it is excluded from bundle identity.

## Security

Static validation fails closed for hidden dependencies, effects, unbounded work,
and unsupported instructions. Diagnostics and generated schemas remain bounded
and redact source values where required.

## Testing

Use valid/invalid source corpora, diagnostic span snapshots with semantic
assertions, parser fuzzing, stable-ID fixtures, IR decoder fuzzing, unsupported
version tests, and repeated-build byte/hash equality for the budget bundle.

## Requirements and Work Packages

- **Requirements:** `ID-003`, `DSL-001` through `DSL-012`, `CMP-001`, `CMP-010`
  through `CMP-013`, `CMP-020` through `CMP-022`, `TXN-001`, `TEST-001`
- **Defines or blocks:** `WP-030`, `WP-040`, `WP-050`, `WP-080`
- **Final evidence:** `WP-140`, `WP-200`

## Decision Deadline

Accept the grammar portion before parser implementation. Accept the complete
record before WP-040 exposes IR or bundle interfaces and fixtures.
