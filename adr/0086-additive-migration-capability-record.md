# ADR-0086: Additive Migration Capability Record

- **Status:** Accepted
- **Direction approved:** 2026-08-01
- **Exact text accepted:** 2026-08-01
- **Acceptance reference:** Human maintainer approval of the proposed additive
  `CapabilityRecordV2` design in the WP-409 implementation session
- **Depends on:** ADR-0006, ADR-0009, ADR-0079

## Context

`CapabilityRecordV1` and `CapabilityPermissionKindV1` have frozen durable schema
hashes. Appending `MigrateContract` to the V1 enum rotates the identity of every
legacy capability record even when the permission is absent. The durable-format
policy requires an additive own-file record instead.

Capability creation, revocation, token lookup reciprocity, and grant loading must
remain atomic. A separate table would add cross-record transaction invariants
without improving the semantic model.

## Decision

Restore `CapabilityRecordV1` and its permission enum byte-for-byte. Add an
own-file `CapabilityRecordV2` containing the unchanged V1 record plus one
required `CapabilityMigrationGrantExtensionV1`.

The V1 base omits every `MigrateContract` permission and its approval-required
kind. The extension contains a nonempty, strictly ordered, duplicate-free list
of exact contract lineages and one boolean recording whether migration requires
approval. The decoder reconstructs one canonical in-memory grant and rejects an
empty, unordered, duplicate, invalid, or inconsistent extension.

Capabilities without migration authority continue to encode as V1. A
capability with migration authority encodes as V2. Both versions remain readable
through the same storage port, and one enveloped table value preserves atomic
create and revoke behavior.

The public Protobuf permission enum is independent and may add
`MIGRATE_CONTRACT = 26`; this ADR changes only the durable representation.

## Compatibility

All legacy V1 schema hashes and wire fixtures remain exact. V2 has its own
record type, schema hash, readable/writable registry entries, and golden
fixtures. Unknown fields, missing extension values, or noncanonical ordering
fail closed.

## Requirements and Work Packages

- **Requirements:** `MIG-006`, `MIG-008`, `MIG-017`
- **Defines or blocks:** `WP-409`
