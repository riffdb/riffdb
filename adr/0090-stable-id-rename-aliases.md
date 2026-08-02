# ADR-0090: Stable-ID Rename Aliases

- **Status:** Accepted
- **Direction approved:** 2026-08-01
- **Exact text accepted:** 2026-08-01
- **Acceptance reference:** Human maintainer approval in the WP-411 implementation session
- **Decision deadline:** Before WP-411 emits a stable-ID-preserving rename
- **Depends on:** ADR-0013, ADR-0076, ADR-0077
- **Amends:** ADR-0013, ADR-0077

## Context

ADR-0013 lineage-ledger version 1 stores exactly one identity key for every
allocated numeric ID and requires each active key to equal the current
executable declaration name. It consequently represents a source rename as a
removal and a new allocation. ADR-0076 later requires a migration-proven
semantic rename to retain the numeric stable ID while permanently preventing
reuse of the predecessor name.

Changing only the active ledger key would forget the predecessor key. Keeping
both keys as ordinary entries would either duplicate the numeric ID or allocate
a false second identity. Neither representation satisfies the accepted stable
ID and tombstone rules.

## Decision

Lineage ledger version 2 adds a canonical rename-alias registry. An alias binds
one historical `StableIdentity` key to the same numeric ID as one current active
entry in the same allocation namespace. It is not an allocation, executable
declaration, tombstone, or second owner of the ID.

A migration-proven semantic rename atomically changes the active identity key,
retains its numeric ID, and appends the predecessor key as a permanent alias.
Further renames append aliases and retain the same ID. Aliases are never
removed, made active, rebound, or reused. A candidate identity that equals any
retained alias fails stable-ID allocation before bundle construction.

Aliases are ordered by the canonical encoded identity key and are unique.
Every alias must resolve to an existing entry in the same exact allocation
namespace, must differ from that entry's current identity, and must not collide
with another active, tombstoned, or alias identity. The target is active when a
rename is created; logical retirement may later tombstone it while retaining
all aliases. Decode and checked construction enforce these invariants.

Existing bundles retain exact lineage-ledger version-1 bytes and meaning.
Ledger version 2 encodes the unchanged allocation sequence followed by a
bounded alias count and ordered alias records. A successor of a v2 ledger
retains all aliases even when it contains no new rename. Unknown ledger
versions fail closed. The bundle format remains version 1 because its ledger is
already an explicitly versioned nested value; generated format documentation
and compatibility fixtures describe both closed ledger alternatives.

The compiler exposes one migration-aware exact-parent successor operation. It
parses and binds rename declarations before stable-ID allocation, compiles the
candidate with those sealed bindings, and then compiles and validates the full
migration bundle against the resulting candidate. No public API accepts an
untrusted caller-authored ID or alias map. Ordinary successor compilation keeps
ADR-0013's removal-plus-addition behavior and cannot infer a rename from source
text alone.

A rename proof is accepted only when predecessor and successor declarations
have the same closed identity kind, exact type, owner role, and semantic role.
Key, partition, aggregate-membership, relationship-target, conflict-domain,
type, or meaning changes are replacements or later-gate changes, never renames.
Historical bundles continue to resolve their original names and bytes exactly.

ADR-0077's previously unimplemented V1 `RenameIdentity` and `RetireIdentity`
step payloads are corrected before Gate-B execution to retain the exact stable
identity owner path: `namespace_tag`, `owner_kind`, `owner_ids`, and
`stable_id`. Namespace tag plus numeric ID is insufficient because fields,
outcomes, and enum variants use owner-scoped numeric sequences. The existing
step tags are preserved. Previously encoded placeholder forms for these two
unexecutable tags are rejected; all Gate-A bundle bytes remain unchanged
because they contain neither tag. This pre-alpha incompatibility is explicit
and prevents lossy or collision-dependent interpretation.

## Consequences

- Stable-ID-preserving renames are explicit and reproducible from exact source,
  parent bundle, and `.riffm` bytes.
- Historical names remain permanently reserved without fabricating allocations.
- Ledger-aware tooling must support versions 1 and 2.
- This is a pre-alpha additive durable-format extension and requires canonical
  codec fixtures, malformed-alias decoder tests, and generated documentation.

## Testing

- Version-1 bundle fixtures decode and re-encode byte for byte.
- Rename fixtures retain entity and field IDs while changing current display
  names and retaining canonical aliases.
- Chained rename and source-declaration reordering produce deterministic bytes.
- Alias collision, rebinding, resurrection, wrong namespace, wrong target,
  duplicate alias, type change, and missing proof fail closed.
- Scoped rename/retirement fixtures prove equal numeric IDs under different
  owners remain distinct in canonical migration bytes and execution.
- Exact historical bundle resolution remains unchanged after cutover.

## Requirements and Work Packages

- **Requirements:** `MIG-001`, `MIG-002`, `MIG-003`, `MIG-009`, `MIG-015`,
  `MIG-019`, `MIG-020`
- **Defines or blocks:** `WP-411`
- **Final evidence:** `WP-413`
