# ADR-0078: Staged Migration Storage, Publication, and Recovery

- **Status:** Accepted
- **Direction approved:** 2026-07-31
- **Exact text accepted:** 2026-07-31
- **Acceptance reference:** Human maintainer exact-text acceptance in the
  implementation session on 2026-07-31
- **Operation-artifact clarification accepted:** 2026-07-31, human maintainer
  confirmation in the WP-408 implementation session
- **Decision deadline:** Before WP-407 freezes migration storage semantics
- **Depends on:** ADR-0076, ADR-0077
- **Amends:** ADR-0003, ADR-0004, ADR-0006, ADR-0010, ADR-0019, ADR-0022,
  ADR-0032, ADR-0042, ADR-0049, ADR-0050, ADR-0061, ADR-0063, ADR-0072,
  ADR-0073

## Context

Broad entity and key migration cannot be one bounded redb transaction, but
RiffDB cannot expose an intermediate row set. In-place restartable mutation
would require live schema generations and rollback machinery. The existing
offline restore design already demonstrates private staging, immutable backup,
checksummed external receipts, atomic publication, and fresh validation.

Migration additionally needs resumable per-batch progress atomically coupled to
row/index changes and must preserve the commit coordinator as the only
authoritative mutator. A public storage callback or server-owned record rewrite
would violate accepted dependency and authority boundaries.

## Proposed Decision

Every apply operation uses this closed selected-database lifecycle:

1. durably accept an exact caller operation ID and semantic input hash;
2. drain the selected database and close its workers, ports, transactions, and
   subscriptions while siblings continue serving;
3. verify the exact parent/candidate/migration artifacts, reject unresolved
   retiring-version Pending admissions, and run complete read-only preflight;
4. create and verify immutable backup
   `pre-migration-<lowercase-uuidv7-operation-id>` using the existing backup
   format and retain it as a normal operator-visible backup;
5. create a private staged database from that exact backup on the target
   database filesystem;
6. apply bounded migration batches and journal progress inside the stage;
7. rebuild required projection generations through the frozen commit frontier;
8. perform complete staged startup, catalog, entity, index, relationship,
   uniqueness, invariant, projection, outbox, and historical-hash validation;
9. atomically install successor activation, write retirement, and the permanent
   migration record in the stage, then publish the staged file through the
   existing parent-synced replacement boundary;
10. freshly open and validate the published target before readiness and terminal
    success become durable.

Preflight mutates neither the database nor backup namespace. It uses bounded
ordered reads after drain, computes output/index counts and a conservative
checked scratch-space requirement, and fails before backup or staging when data,
version, resource, disk, or pending-admission conditions do not pass. Apply
always repeats preflight even if a prior check operation succeeded.

Staging uses a protected sibling path on the same filesystem as the configured
database, never `/tmp` or a caller path, so final rename is atomic. External
receipt files remain under the selected database's reserved
`backup_root/.maintenance` subtree. Existing lexical path-disjointness and
symlink rejection apply.

Before drain, the server publishes the exact canonical successor contract
bundle and migration bundle as immutable operation artifacts under the
protected `.maintenance/migrations/<operation-id>/` subtree. Each artifact and
its parent are synced before acceptance. The receipt binds each artifact's
exact length and SHA-256 checksum. Restart reconstructs and revalidates the plan
only from those artifacts plus the exact predecessor backup; it never requests
caller resubmission. The artifacts remain private and are not embedded in the
bounded receipt because their combined accepted maximum exceeds 30 MiB.

`ContractMigrationReceiptV1` is a versioned checksummed canonically encoded
external operation state. It binds the database identity, operation ID/kind,
semantic input hash, exact artifacts, source backup name/manifest hash, stage
identity, monotonic phase, result or bounded safe failure, and at most 32 phase
transitions. It contains no credential or row value.

`StoredContractMigrationJournalV1` is a versioned in-database stage record. It
binds the same operation/artifacts and carries the current typed step, canonical
exclusive cursor, accumulated checked counts, frozen frontiers, and previous
journal hash. Each batch transaction atomically compares the expected journal,
rechecks every input row version/hash, applies at most 64 row mutations under
the existing encoded transaction bound, updates all affected indexes and
generations, and advances the journal. Scan pages contain at most 256 rows.

The additive V1 durable layout uses exactly four tables:
`contract_migration_journal`, keyed by the 16-byte operation UUID;
`contract_migrations`, keyed by that UUID;
`contract_write_retirements`, keyed by the 32-byte predecessor bundle hash; and
`retired_entities`, keyed by operation UUID followed by a big-endian `u32`
length and the canonical entity-target bytes. The corresponding writable
envelope records are `StoredContractMigrationJournalV1`,
`StoredContractMigrationRecordV1`, `StoredContractWriteRetirementV1`, and
`StoredRetiredEntityRecordV1`, defined additively in `migration_v1.proto`.

The commit crate owns a separate `MigrationCoordinator`. Catalog supplies a
sealed validated migration plan and schema proof. The coordinator evaluates
row transforms outside the write transaction, then rechecks and applies them
through one sealed migration transaction port. Storage API owns value-only
evidence, bounds, journal/record DTOs, and identity-only ports; it imports no
compiler or catalog authority. Redb owns file/table mechanics only. The server
joins exact authorities and orchestrates lifecycle; it cannot construct a
migration batch.

The final staged cutover assigns exactly one `AdministrationSequence` through
the commit-owned coordinator and atomically persists successor activation, the
write fence, one terminal `StoredServiceAuditRecordV1` for
`ApplyContractMigration`, and `StoredContractMigrationRecordV1`. The migration
record contains exact artifacts, source backup/manifest identity, operation,
actor and authorizing capability identity/revision, predecessor and successor
frontiers, per-step counts, terminal validation digest, and administration
sequence. Operational wall time may appear in those administration records but
never enters row transforms or artifact hashes. A failed pre-cutover operation
assigns no administration sequence; its external receipt remains the recovery
and audit evidence. Retired data requiring separation from active key/index
namespaces uses versioned `StoredRetiredEntityRecordV1` keyed by migration and
original canonical target; it is not publicly writable or physically reclaimed
by this feature.

Crash reconciliation uses external receipt, protected stage, backup manifest,
published migration record, and target filesystem evidence. Before publication,
the original target is authoritative and recovery resumes the exact stage or
recreates it from the exact backup when journal/stage evidence is absent. After
publication, the published target is authoritative only if fresh validation and
the migration record agree. Otherwise recovery automatically restores the exact
operation backup, freshly validates it, and durably reports `FailedRolledBack`
before admitting any selected-database request. No automatic rollback occurs
after terminal success.

Migration never changes the application sequence allocator. Batches consume no
administration sequence; only the successful final cutover consumes the one
sequence described above. It never changes `HistoryIncarnation`, because
successful migration preserves one forward history rather than rewinding it.
Automatic rollback follows ADR-0072 restore-rewind semantics and must install a
fresh incarnation before readiness so every pre-publication observation is
fenced.

## Options Considered

1. **In-place migration with live generations:** Deferred until online
   migration because it adds dual-read/write and rollback semantics.
2. **One unbounded transaction:** Rejected because it violates bounded storage
   and recovery requirements.
3. **External journal only:** Rejected because batch progress would not be atomic
   with transformed rows.
4. **Staged copy plus external receipt and internal journal:** Proposed because
   it provides invisible bounded progress, restart, atomic publication, and a
   verified rollback point using existing maintenance foundations.

## Consequences

- Apply requires downtime and conservative temporary disk capacity for target,
  backup, stage, archive, and redb growth.
- The permanent backup is never automatically deleted.
- Process recovery gains a new known maintenance state but siblings may start
  once every configured database has structurally proved either Ready or a
  valid resumable migration state.
- New durable DTOs, table keys, envelope tags, integrity evidence, and recovery
  fixtures require exact acceptance before implementation.

## Compatibility

Existing backup/restore receipts and storage records retain exact bytes. New
migration records use new versioned tags/tables and registry fixtures. Unknown,
mixed, mismatched, truncated, noncanonical, or impossible migration state blocks
the selected database and never triggers inferred repair.

## Security

Protected paths reject symlinks and caller-controlled traversal. Credentials are
never persisted in receipts or stages. Backup and stage access remain concrete
server/storage responsibilities below API-neutral authorization, and public
responses contain names/hashes only after authorization and obligation handling.

## Testing

- Memory/redb conformance for every batch, cursor, count, and validation rule.
- Durable codec goldens plus parser fuzzing for receipt, journal, record, archive,
  table keys, and impossible phase pairs.
- Process failpoints before/after every receipt sync, drain, backup, stage,
  batch, projection step, activation, publication, validation, rollback, and
  terminal sync.
- Disk exhaustion, entity-version exhaustion, journal mismatch, corrupt stage,
  corrupt backup, kill/restart, and two-database isolation tests.
- Architecture scans reject server/storage row construction and public generic
  migration ports.

## Requirements and Work Packages

- **Requirements:** `MIG-007` through `MIG-016`, `MIG-020`
- **Defines or blocks:** `WP-407` through `WP-413`
- **Final evidence:** `WP-413`

## Decision Deadline

Exact human acceptance is required before WP-407 adds a migration coordinator,
durable migration record, table key, journal, receipt, stage, or recovery path.
