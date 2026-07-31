# ADR-0066: Additive Contract Evolution and Deployment Diagnostics

- **Status:** Accepted
- **Date:** 2026-07-30
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `EVL-001` through `EVL-006`
- **Related work package:** `WP-393`
- **Amends:** ADR-0002, ADR-0007, ADR-0013, ADR-0028, ADR-0041,
  ADR-0046, ADR-0064

## Context

The first independently authored application could add commands but could not
deploy a successor that added an enum, entity, aggregate, or enum variant. The
compiler produced a checked candidate, but the compatibility comparator
classified every such declaration as an unsupported addition. The deployment
service then collapsed both source compilation failures and incompatible
successors into a root `invalid_value` validation error with no diagnostic.

This behavior is fail-closed but prevents normal additive application
evolution and gives an operator or agent no safe correction path. The
maintainer approved this record's exact rules on 2026-07-30 and confirmed that
the current pre-alpha dogfood database will be reset after installing the
updated binaries.

## Decision

### Additive schema evolution

A successor may add:

- a new enum with its complete initial variant set;
- a new entity with its complete initial key, fields, indexes, and
  entity-local invariants;
- a new aggregate whose root and every child are entities introduced by the
  same successor, including its complete initial aggregate invariants; and
- relationships and unique constraints whose ownership and referenced schema
  are confined to entities introduced by the same successor.

These changes are `Compatible`. Existing source and durable identities remain
stable; the new declarations have fresh stable IDs and begin with no rows.

Appending a fresh variant to an existing enum is
`RequiresExplicitVersion`. The candidate must still use a successor contract
version and deployment must compare-and-swap against the exact expected active
version. Existing callers pinned to an older exact contract retain their
closed enum. No caller may infer compatibility from a numeric version alone.

Removing, renaming, reusing, or changing an existing enum variant, entity key,
field type, index, invariant, relationship, unique constraint, aggregate
membership, partition derivation, or conflict derivation remains
`Incompatible`. Adding an index, relationship, invariant, or uniqueness rule
to an existing entity remains incompatible until a separate migration design
exists.

The compatibility registry adds distinct stable codes for added enums,
entities, aggregates, and enum variants. Activation accepts both
`Compatible` and `RequiresExplicitVersion` reports after the catalog has
verified exact expected-active-version compare-and-swap. It never activates an
`Incompatible` report.

### Deployment diagnostics

Contract deployment returns ordinary structured results for:

- source compilation failure, carrying the same bounded safe syntax or
  semantic diagnostics as contract validation; and
- an incompatible successor, carrying the checked candidate descriptor and
  bounded compatibility summary.

These results are owned by the API-neutral application service and are mapped
without semantic reinterpretation by gRPC, CLI, SDK, hosted MCP, and stdio MCP.
They are emitted only through the existing authorization, redaction, response
budget, and audit boundaries. Internal compiler, catalog, storage, or
dependency prose is never released.

A compatibility-proof mismatch after the service has accepted a checked
descriptor is an integrity failure, not a user validation result.

### Reset boundary

The POC does not add online data migration, destructive deployment, or a
runtime database-reset RPC. Pre-alpha dogfood databases may be reset only by
stopping the service, preserving any desired backup, removing or replacing the
selected database through documented operator-controlled filesystem
procedures, and redeploying a complete genesis contract. Reset is an explicit
loss-of-data operation and is never attempted automatically by deployment.

## Options Considered

1. **Keep every schema addition incompatible:** Rejected because independently
   authored applications could not evolve without wiping data even when new
   declarations cannot reinterpret existing rows.
2. **Treat every additive declaration as compatible:** Rejected because adding
   variants to an existing closed enum changes exhaustive behavior, and adding
   constraints or indexes to existing entities requires migration semantics.
3. **Permit the bounded additive subset in this record:** Accepted because it
   enables useful evolution while preserving existing row, plan, and
   authorization meanings.
4. **Expose compiler or catalog error strings directly:** Rejected because
   internal prose is not a bounded, redaction-safe public contract.

## Consequences

- Applications can introduce new domains and storage shapes without resetting
  an otherwise compatible database.
- Existing closed enums require explicit version selection before a new
  variant can be observed.
- Existing schema remains deliberately frozen until migration semantics are
  designed.
- Deployment clients gain additive structured result variants and must
  regenerate public fixtures.
- The current dogfood reset removes its diagnostic-only successor history; it
  is not evidence for online migration.

## Compatibility

The new compatibility codes extend the versioned contract-IR compatibility
registry and checked bundle fixtures. Their meanings are immutable once
merged. The deployment response variants are additive pre-alpha Protobuf, MCP,
SDK, and CLI-v1 interface changes with regenerated compatibility fixtures.
No authoritative storage key, entity row, commit record, idempotency identity,
or executable plan encoding changes.

## Security

The service continues to authenticate and authorize deployment before
returning candidate-specific compatibility detail. Diagnostics contain only
bounded compiler-owned codes, authorized names, source spans, and safe
remediation. Transports do not compile independently, touch catalog storage,
or turn an incompatible result into activation.

## Testing

- IR comparator tests freeze every new code/class and reject additions that
  touch existing schema.
- Compiler fixtures cover an EA-shaped successor adding an enum variant, new
  entity, aggregate, commands, and query-facing schema.
- Catalog tests prove exact-version activation of
  `RequiresExplicitVersion`, rejection of `Incompatible`, and replay/CAS
  precedence.
- Service and transport conformance tests freeze structured syntax, semantic,
  and incompatibility results with authorization and redaction assertions.
- Generated Protobuf, MCP schema, CLI JSONL, and compatibility artifacts must
  reproduce exactly.

## Requirements and Work Packages

- **Requirements:** `EVL-001` through `EVL-006`
- **Defines and blocks:** `WP-393`
- **Final evidence:** `WP-393`
