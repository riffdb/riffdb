# ADR-0013: Stable IDs, IR Encoding, and Plan-Hash Framing

- **Status:** Proposed
- **Direction approved:** No
- **Exact text accepted:** No
- **Decision deadline:** Before WP-040 public interfaces or fixtures merge

## Context

ADR-0002 freezes grammar version 1 and the AST/HIR/IR ownership boundary without
guessing the durable executable representation. WP-040 must decide how equal
source produces equal IDs, bundles, schemas, and plan hashes while later
compatible contract versions retain lineage-stable IDs and historical plans.

## Questions to Resolve

1. How first-version IDs are assigned deterministically across declaration kinds.
2. How later compilation consumes lineage history, allocates new IDs, and records
   tombstones so removed IDs are never reused.
3. Which IR envelope, version tags, instruction tags, ordering rules, and bounds
   are durable compatibility boundaries.
4. Which previous IR versions one binary executes, migrates, or rejects.
5. The exact canonical inputs and framing for plan, schema, and bundle hashes.
6. How compiler version, grammar version, IR version, application version, source
   hash, options, and compatibility metadata enter canonical bundle bytes.
7. How unsupported instructions and versions fail closed without partial plans.

## Options to Evaluate

- Explicit source IDs versus deterministic first-version assignment plus lineage
  allocation metadata.
- Protobuf IR under the durable envelope versus a separate checked canonical
  codec.
- One-version execution versus a bounded reader/executor compatibility window.
- Per-command plan hashes plus bundle root versus one monolithic bundle hash.

## Decision

Not yet decided. WP-040 may prototype fixtures outside public interfaces, but it
must not merge stable ID allocation, executable tags, durable IR bytes, or plan
hashes until the maintainer accepts this record or a replacement.

## Requirements and Work Packages

- **Requirements:** `ID-003`, `CMP-001`, `CMP-020` through `CMP-022`, `DSL-003`
  through `DSL-012`, `TXN-001`
- **Defines or blocks:** `WP-040`, `WP-050`, `WP-080`
- **Final evidence:** `WP-140`, `WP-200`
