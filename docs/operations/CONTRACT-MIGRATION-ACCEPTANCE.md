# Contract Migration Acceptance

WP-413 closes the P7 offline contract-migration gate. This page is the review
map for operators and maintainers; the authoring and command workflow remains
in [Contract Migrations](../contracts/MIGRATIONS.md).

## Supported evolution matrix

| Gate | Supported change classes | Final focused evidence |
|---|---|---|
| A | Required-field backfill, index, relationship, unique rule, invariant, new projection | `contract_migration_gate_a` |
| B | Semantic rename, logical retirement, checked field/type replacement, exhaustive enum map, replacement projection | `contract_migration_gate_b` |
| C | Primary-key replacement, rekey, repartition, aggregate ownership, relationship target, conflict domain | `contract_migration_gate_c` |

`contract_migration_acceptance` decodes and seals the canonical artifact triple
for every gate. It also proves that two retained predecessor bundles can each
name a direct migration to the same exact successor. RiffDB never infers or
chains an intermediate migration.

Application Source V1, V2, and V3 and Application Lock V1, V2, V3, and V4 are
included in the final strict decode/round-trip matrix. V4 is the only lock that
can carry migration entries; reading an older format does not invent migration
authority or artifacts.

## Installed and operational matrix

| Boundary | Evidence | Required result |
|---|---|---|
| User source install and generic bootstrap | `./scripts/release-source-bootstrap-smoke` | Private user layout, real binaries, two database aliases, restart-safe bootstrap, no system-boundary access |
| System package layout | `./scripts/release-install-smoke` and `./scripts/release-systemd-smoke` as run by release verification | Exact binaries, units, sysusers/tmpfiles, file modes, sandbox paths, supervised closed-stdin daemon |
| Populated public migration | `contract_migration_gate_a` | gRPC-only check/apply/poll, exact retry, migrated rows, retained history, projection readiness |
| Multi-database isolation | `contract_migration_gate_a` and `contract_migration_recovery` | Selected database drains while sibling remains usable before and after restart |
| Crash and rollback | ignored `contract_migration_recovery` target | Exact resume or validated predecessor/successor; invalid publication rolls back before readiness |
| Authorization and redaction | WP-409 service, gRPC, CLI, and MCP conformance tests | Only exact `MigrateContract` authority succeeds; MCP and application drivers remain migration-free |
| Backup retention | Gate A durable inspection and recovery matrix | Verified `pre-migration-<operation-id>` backup remains after success |

The automated source installer runs live only in user scope. The release suite
stages and verifies the system layout and units without modifying the host's
`/etc`, `/usr`, or system manager. A real system installation still requires
the disposable-host acceptance described in [Known Limitations](../known-limitations.md).

## Dogfood and comparison

`examples/migration-evolution` contains an assistant-style EA evolution that
adds the `Email` origin and `EmailDraft` entity while backfilling and indexing
populated attention rows. The final acceptance test compiles and seals that
exact migration alongside the populated TicketDesk public workflow.

`scripts/migration-evolution-comparison` runs the RiffDB matrix with fixed
bounds and optionally applies the corresponding SQL evolution to a disposable
PostgreSQL schema. PostgreSQL failure controls remain correctness evidence and
are never included in performance ratios. Results are revision- and
environment-specific; see the checked report and methodology in
[WP-413 Migration Evolution Evidence](../performance/wp-413-migration-evolution.md).

## P7 review result

The automated gate covers `MIG-001` through `MIG-020` through the owning
packages and the final evidence targets below. It does not broaden the feature:

- migration remains offline, single-database, and process-wide exclusive;
- committed history is immutable and migration assigns no application commit;
- only the commit-owned coordinator constructs authoritative migration writes;
- invalid predecessor data is repaired with ordinary predecessor commands;
- successful cutover keeps its verified backup and installs an old-writer fence;
- MCP, TypeScript, Python, and generated application clients expose no migration;
- there is no SQL, callback, online dual-write, cross-row split/merge, physical
  purge, or cross-database migration path.

Run the complete gate from a clean checkout:

```bash
TMPDIR="$HOME/tmp" cargo test --workspace --all-features
TMPDIR="$HOME/tmp" cargo test -p riffdb-server \
  --test contract_migration_recovery -- --ignored
TMPDIR="$HOME/tmp" cargo test -p riffdb-testkit-server \
  --test contract_migration_acceptance --all-features
./scripts/release-source-bootstrap-smoke
./scripts/handbook check
./scripts/check-application-bindings
./scripts/check-generated
./scripts/check-requirement-coverage
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo deny check
```

P7 is accepted only when all commands pass at the reviewed revision. A failed
or skipped command keeps the gate open.
