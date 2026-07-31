# ADR-0083: Authoritative Entity References

- **Status:** Accepted
- **Date:** 2026-07-31
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `STO-001`, `TXN-004`, `REC-003`
- **Related work packages:** Package E (format acceptances)
- **Amends:** ADR-0022/ADR-0061 (commit record contents), following the
  event-reference precedent established in the storage-v2 line

## Context

The commit record still embeds the full post-image of every mutated entity.
Each entity write is therefore durably stored twice — once as the
authoritative entity row and again inside the append-only commit log —
doubling entity write bytes forever in the one table retention cannot touch.
A completed audit proved no consumer requires historical post-image bytes:
the only byte-exact comparisons are against the current entity row, the
public commit surface exposes only entity key and version, projections
consume event payloads (already references), and recovery reads stored
outcomes only. Durable events already solved the identical problem with
hash-bearing references. After alpha, this change becomes a migration of
users' authoritative history; before alpha it is a bounded format revision.

## Decision

### Commit records reference entity post-states

The commit record's mutation list is replaced by entity references, each
carrying exactly: the entity target, the committed entity version, and a
keyed hash of the complete post-image (a new closed hash domain over the
canonical entity record preimage). Contract attribution and schema binding
are not carried — they are byte-derivable from the commit's own executable
plan reference — and the expected pre-state is a total function of the
committed version. Validation strength is unchanged:

- Staging proves each reference is derived from the staged post-image before
  the atomic commit.
- Startup proves, in one forward pass over the commit log, that every
  entity's reference chain is version-contiguous from first write and that
  the terminal reference's hash equals the hash of the current entity row.
  Every commit's references are still validated; every current entity is
  still proven to be the product of its history (ADR-0019 scope unchanged).
- Chain violations are reported per entity and never mask sibling entities.

### Versioning and migration

The new commit record is a new revision of the existing durable role in its
own proto file. The previous revision remains readable; decoding it derives
references by hashing the embedded post-images. A registry-digest migration
step transcodes existing commit rows in bounded, crash-restartable pages,
self-contained with no entity-table join. Databases at any earlier digest
converge through the existing migration chain.

### Explicitly out of scope

Entity rows themselves are unchanged; no per-row hash is stored. The public
commit surface is unchanged (it already exposes key and version only).

## Consequences

- Entity payload bytes are written once per command instead of twice; the
  commit log shrinks by the full post-image per mutation.
- The startup entity-history check becomes linear in commits plus entities
  instead of entities times commits.
- Historical post-image bytes are no longer reconstructable from the commit
  log alone; the audit confirms nothing requires them. Any future feature
  needing them is a new decision with its own record.
- Write-bytes evidence baselines are re-measured, not threshold-adjusted.

## Rejected alternatives

- **Carry contract/binding fields in the reference.** Byte-duplicates plan
  fields the commit already stores.
- **Store the post-image hash on the entity row.** Rotates a frozen schema for
  a value derivable on demand.
- **Accept the duplication.** Permanent doubled entity write amplification in
  the unprunable log, with the cost of reversal growing with every commit.

## Acceptance

Accepted by the maintainer on 2026-07-31.
accepted.
