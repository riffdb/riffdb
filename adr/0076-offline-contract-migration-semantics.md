# ADR-0076: Offline Contract Migration Semantics and Cutover

- **Status:** Accepted
- **Direction approved:** 2026-07-31
- **Exact text accepted:** 2026-07-31
- **Acceptance reference:** Human maintainer exact-text acceptance in the
  implementation session on 2026-07-31
- **Decision deadline:** Before WP-406 freezes migration compatibility or IR
- **Amends:** ADR-0002, ADR-0003, ADR-0005, ADR-0007, ADR-0013, ADR-0052,
  ADR-0057, ADR-0066, ADR-0075

## Context

ADR-0066 permits additions that cannot reinterpret existing state and keeps
indexes, constraints, types, keys, ownership, partitioning, conflict derivation,
and removals on existing schema incompatible until a migration design exists.
That boundary is correct for ordinary deployment, but forces a reset for useful
schema evolution after an application contains authoritative data.

Migration cannot be an application write bypass, reinterpret committed history,
or make partially rewritten state visible. Key and conflict-domain changes also
cannot safely coexist with newly admitted predecessor commands. The design must
therefore define a distinct administrative transition while preserving the
commit coordinator's sole authoritative-mutation ownership.

## Proposed Decision

Add a fourth compatibility class, `RequiresMigration`, between
`RequiresExplicitVersion` and `Incompatible`. The IR-owned comparator classifies
an intrinsically migratable successor with stable codes. Ordinary deployment
MUST return a structured `MigrationRequired` result and perform no mutation.
Only an exact checked migration bundle may prove coverage and admit the
successor to the migration path. A missing, partial, excessive, ambiguous, or
wrong-parent proof fails closed.

The first complete framework supports these changes in three implementation
gates without changing the frozen v1 meaning of an unimplemented IR step:

1. required-field backfills and additions of indexes, relationships, uniqueness
   rules, entity-local invariants, and new projections over existing schema;
2. semantic-preserving renames, logical retirement, replacement fields with
   checked conversions, exhaustive enum remapping, and replacement projections;
3. primary-key replacement, rekeying, repartitioning, aggregate-membership
   changes, and conflict-domain changes.

A rename preserves a stable ID only when the exact type and semantic role remain
unchanged. A type or meaning replacement allocates a fresh stable ID and
tombstones the prior identity. Removal is logical: retired identities, original
bundle interpretations, and retained data remain available for integrity,
historical inspection, and rollback. There is no migration-time physical purge.

Migrations transform current entity, catalog, index, and derived projection
state. They MUST NOT rewrite committed outcomes, events, provenance,
idempotency records, commit records, application sequences, event hashes, or
historical bundle bytes. A changed entity image advances `EntityVersion`
exactly once for that migration and records the successor contract binding.
Migration creates no `CommitSequence`, command outcome, durable event, outbox
intent, or command provenance. One permanent migration record supplies bounded
administrative provenance and exact artifact/backup identities.

Migration is an exclusive administrative operation, not an application
operation. A commit-owned `MigrationCoordinator` is the sole component allowed
to apply checked authoritative migration batches or the final catalog cutover.
The server may orchestrate lifecycle, backup, staging, validation, and file
publication, but it cannot construct row mutations or write catalog state.
Storage implementations expose only sealed, migration-specific semantic ports;
there is no generic row editor, callback, or public storage handle.

Every migration drains exactly one selected database. Other configured
databases remain available. The selected database admits no application,
worker, subscription, or ordinary administration operation until migration
finishes or rolls back. Only one migration check or apply runs process-wide.

Cutover installs a durable predecessor-write fence. No new mutating command
under the migrated lineage at or below the predecessor version may be admitted.
Terminal outcome resolution and historical commit/provenance inspection remain
available. A predecessor read-only operation may run only when the catalog
proves that all referenced identities retain exact type and meaning. Otherwise
it returns a structured retired-version result.

Drain/preflight MUST reject any unresolved durable `Pending` admission belonging
to a version that the cutover would retire. It may not execute that predecessor
write after cutover or strand its uncertainty recovery. The operator repairs
invalid data only through ordinary compiled predecessor commands and retries the
migration; migration has no skip, fallback, or generic repair mode.

Authoritative cutover is externally atomic because all changes are made to a
private staged database. Batch transactions inside the stage are restartable,
but no application can observe them. Projection generations required by the
successor are rebuilt through the frozen application frontier and published in
the staged database before file publication.

## Options Considered

1. **Online dual-schema migration:** Rejected for the first release because
   dual writes, predecessor command translation, and rolling storage generations
   multiply the correctness surface before single-node migration is proven.
2. **Direct engine or CLI edits:** Rejected because they bypass compiler,
   authorization, coordinator, audit, and recovery boundaries.
3. **One application commit per row:** Rejected because migration is not a
   business command and must not synthesize outcomes, events, provenance, or
   application sequence history.
4. **Rewrite historical events/outcomes:** Rejected because it invalidates
   durable hashes, locators, replay, and compatibility evidence.
5. **Exclusive staged migration with predecessor-write retirement:** Proposed
   because it supports broad evolution while preserving one visible state and
   immutable history.

## Consequences

- Schema evolution becomes useful without an online migration protocol.
- Migration downtime and temporary disk amplification are explicit.
- Breaking cutover intentionally rejects old writers rather than translating
  them silently.
- Cross-row split, merge, aggregation, general scans, SQL, host-language code,
  and automatic physical reclamation remain unsupported.
- Existing compatibility and deployment formats require additive versioned
  extensions and regenerated fixtures.

## Compatibility

Existing `Compatible`, `RequiresExplicitVersion`, and `Incompatible` codes keep
their meanings. New codes and `RequiresMigration` are additive pre-alpha IR and
public presentation changes. Migration-authorized identity replacement never
reuses a tombstoned numeric ID. Historical bundle, row, commit, event, outcome,
and provenance encodings are preserved byte-for-byte.

## Security

Migration requires the dedicated authority defined by ADR-0079. Application,
MCP, deploy-only, query, and capability-administration roles do not imply it.
Diagnostics are bounded, value-free by default, authorization-filtered, and
redacted before public release or telemetry.

## Testing

- Compatibility goldens freeze every new code and coverage rule.
- Model tests compare migrated entity/catalog/index state to a pure reference.
- Negative tests prove old writes, unresolved Pending admissions, incomplete
  proofs, history rewrites, generic edits, and skipped invalid rows fail closed.
- End-to-end tests assert immutable commit/event/outcome/provenance/idempotency
  bytes, unchanged application sequence, exact entity-version advancement, and
  projection frontier readiness.

## Requirements and Work Packages

- **Requirements:** `MIG-001` through `MIG-005`, `MIG-009`, `MIG-011`,
  `MIG-013` through `MIG-015`, `MIG-019`, `MIG-020`
- **Defines or blocks:** `WP-406` through `WP-413`
- **Final evidence:** `WP-413`

## Decision Deadline

Exact human acceptance is required before WP-406 adds a compatibility class,
migration syntax, migration IR, or application lock format.
