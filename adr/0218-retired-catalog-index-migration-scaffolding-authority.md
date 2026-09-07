---
adr: "0218"
title: Retired Catalog-Index Migration Scaffolding Authority
status: proposed
tier: guarantee
date: 2026-09-07
accepted: null
requires: [ADR-0181, ADR-0204]
amends: [ADR-0204]
supersedes: []
requirements: [GOV-001, GOV-002, GOV-003, AFC-005, AFC-006, VER-005, STO-020]
packages: [WP-757]
obligations: []
review_triggers:
  - The edit to crates/riffdb-commit/src/initialization.rs would escape its cfg(test) module or change initialization, probe, permit, structural-open, storage, commit, or application behavior.
  - A StoredIndexEntryV1 reader, writer, semantic row, migration port, migration-required outcome, V1 rewrite, driver, backend, fixture, dispatch, or compatibility alias would remain after WP-757.
  - Current StoredIndexEntryV2 decoding, exact historical-partition validation, startup bounds, epoch-first refusal, or the retained STO-020 proof would change.
  - Another guarantee path, durable identity, byte, key, table, transaction, public operation, or runtime migration behavior would change under this amendment.
---
# ADR-0218: Retired Catalog-Index Migration Scaffolding Authority

## Context

ADR-0204 retires `StoredIndexEntryV1` and requires every associated reader,
fixture, topology identity, and dispatch to disappear in WP-757. The current
reader already accepts only `StoredIndexEntryV2`, but the repository still
contains an unreachable catalog-index migration chain: a V1 semantic row, a
V1 rewrite instruction, migration-required outcomes, a migration port and
driver, a redb backend, server dispatch, and recovery fixtures. Retaining that
chain as generic scaffolding would violate ADR-0204 Decision 8's prohibition on
retired placeholders.

Coherent deletion also removes the migration associated type from structural
startup. The only resulting guarantee-path edit not already named by ADR-0204
Decision 2 is test scaffolding inside
`crates/riffdb-commit/src/initialization.rs`. Its production initialization
boundary does not use the migration port. ADR-0204 requires separately accepted
authority before that path may change.

## Decision

1. ADR-0204 Decision 2 is amended only to authorize an edit to
   `crates/riffdb-commit/src/initialization.rs`, solely inside its existing
   `cfg(test)` module, to follow the removal of the retired structural-startup
   migration associated type and migration-required outcome. The production
   prefix of that file, its public types and methods, compile-fail examples,
   initialization sequence, probe and permit ownership, structural-open
   sequencing, and error behavior remain byte-for-byte unchanged.

2. The test-only edit removes `StartupIndexMigrationPort` imports,
   `FakeMigrationPort`, its trait implementation, the
   `StructuralEvidenceSession::MigrationPort` associated type, and the migration
   type argument from `StructuralOpenOutcome`. Existing fake storage identity,
   session identity, evidence pages, finish behavior, unavailable errors, call
   counts, assertions, and requirement coverage remain exact. No replacement
   dummy, marker, generic parameter, alias, or placeholder is introduced.

3. WP-757 coherently deletes the unreachable catalog-index V1 migration chain
   already covered by ADR-0204 Decisions 5 and 8: `StoredIndexEntryV1` and its
   codec; the V1 semantic row and `is_v1` flow; `StartupIndexMigrationPort`;
   migration-required structural and catalog outcomes; every
   `CatalogIndexMigration*` type, V1 rewrite and V2 confirmation driver path;
   the redb migration backend and export; server migration dispatch; and their
   architecture, unit, simulation, benchmark, and recovery scaffolding. A
   retained generic shell is not an acceptable substitute for deletion.

4. The surviving startup path decodes only the exact current
   `StoredIndexEntryV2` identity, produces only the current V2 semantic row, and
   validates its exact historical partition before returning the existing
   ready/clean outcome. `inspect_index_row`, `index_historical_evidence`,
   `read_historical_evidence`, their page and byte bounds, session binding, and
   fail-closed corruption behavior remain exact.

5. The retained req-tagged proof
   `current_index_rows_require_the_exact_historical_partition` continues to own
   STO-020 current-row partition validation. Existing OBL-0204-1 proof
   `epoch_two_format_gate_precedes_retired_reader_dispatch` is strengthened to
   assert the structural absence of `StoredIndexEntryV1`, `V1Rewrite`, every
   `CatalogIndexMigration*` symbol, `StartupIndexMigrationPort`, the redb
   migration backend, and migration-required startup dispatch. No historical
   migration test is renamed to claim current behavior.

6. After exact acceptance, a separate governance-only commit adds this ADR to
   `WP-757.required_adrs` and its exact record path to `allowed_paths`. It
   changes no implementation, accepted obligation, package dependency,
   requirement, closure, or runtime behavior. Implementation remains a distinct
   guarantee-tier WP-757 commit.

7. ADR-0204 otherwise remains exact, including its immutable epoch-first gate,
   32 retired identities, 26 deleted declarations, five removed historical
   rows, 27 descriptor-closure rotations, tag-65 retirement, current tag-67
   semantics, export/reimport ceremony, storage keys, transactions,
   acknowledgement, recovery, bounds, and public refusal.

## Options considered

1. **Keep a dummy migration associated type:** rejected because it preserves a
   retired identity as unreachable placeholder machinery.
2. **Edit the guarantee path under the package trailer alone:** rejected because
   ADR-0204 expressly requires separately accepted authority for a third path.
3. **Authorize the exact test-only cleanup:** chosen because it permits coherent
   deletion without changing the production initialization boundary.

## Consequences

- WP-757 can remove the complete obsolete migration chain instead of leaving a
  false generic shell solely to avoid a guarantee-classified test edit.
- Current V2 historical evidence and its STO-020 proof remain the only
  catalog-index startup semantics.
- This record adds no reader, migration, compatibility mode, storage behavior,
  public surface, or runtime authority.

## Standing design tests

- **Interface safety:** no application, agent, operator, transport, test, or
  configuration gains a migration choice, old-reader selector, rewrite input,
  initialization capability, or fallback. The only new path authority is an
  exact deletion inside an existing private test module.
- **Scale:** removing the migration chain adds no work or state. Current startup
  retains its existing bounded evidence pages and exact historical-partition
  validation, with no population rewrite or compatibility scan.

## Checks

- `epoch_two_format_gate_precedes_retired_reader_dispatch` freezes complete
  retired-symbol absence and the unchanged epoch-first refusal.
- `current_index_rows_require_the_exact_historical_partition` proves exact
  current V2 row validation and rejects every incomplete or excess partition.
- Commit initialization tests prove the unchanged production type-state
  boundary after their obsolete migration associated type is removed.
- Version-topology, requirement, obligation, allowed-path, file-size, panic,
  workspace, clippy, recovery, and `ci-all` checks retain ADR-0204's guarantees.
