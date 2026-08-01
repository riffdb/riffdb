# Contract migrations

RiffDB distinguishes a compatible contract successor from one that requires
existing authoritative rows or derived state to be checked or transformed.
The latter is `RequiresMigration`: the candidate is valid, but deployment does
not activate it without an exact migration artifact for the active parent.

The current implementation compiles and locks Gate A migration artifacts and
can inspect them locally. It also contains the internal redb execution and
startup-recovery path: complete preflight, an immutable automatic backup,
bounded staged transforms, projection rebuild, complete validation, atomic
publication, and automatic post-publication rollback. `riffdb migration plan`
remains read-only. Public authorization, check/apply/status RPCs, SDK methods,
and CLI commands are not available until WP-409; WP-410 proves the installed
Gate-A workflow end to end.

## Semantic execution boundary

The catalog rechecks the exact parent, candidate, migration hash, direct
compatibility, and complete Gate-A proof before producing a validated plan.
Migration expressions run as pure row-local evaluations. Apply repeats the
whole read-only preflight before it constructs any mutation.

The commit-owned migration coordinator is the only first-party caller of the
stage mutation and cutover ports. It scans at most 256 rows per page and applies
at most 64 row/index transitions per atomic journal step. Every transition
rechecks the exact target, entity version, schema binding, canonical bytes, and
canonical row hash. Required projection generations are tied to the frozen
application frontier before final cutover.

Gate-A required-field transforms advance each changed entity version exactly
once and bind that post-image to the successor. Index-only and validation-only
work does not advance entity versions. Migration never assigns an application
commit sequence or rewrites command, outcome, event, provenance, idempotency,
outbox, or retained history bytes. Only successful final cutover assigns one
administration sequence in the reference model.

These semantics are not a hidden storage-edit API. The memory stage remains the
reference model. The redb implementation realizes the same sealed ports; only
the commit-owned coordinator can construct batches or cutover, and neither the
server recovery controller nor a storage adapter interprets migration IR.

## Durable redb lifecycle

An accepted operation durably binds a UUIDv7 operation ID, canonical input
hash, exact parent/candidate/migration hashes, exact protected artifact files,
and restart-stable admission evidence. Same operation ID with different input
fails before drain. The receipt uses a closed monotonic phase graph and contains
no row value, credential, token, or absolute path.

After the selected database drains, apply repeats the complete read-only
preflight before allocating disk or creating a backup. RiffDB reserves a
conservative amount of space, creates and verifies the immutable normal backup
`pre-migration-<operation-id>`, and materializes a protected sibling stage on
the target filesystem. It never stages through `/tmp`, accepts a caller-selected
path, follows a symlink, or publishes across filesystems.

A deterministic artifact, predecessor-data, pending-admission, capacity, or
disk preflight rejection records a value-free `FailedClosed` reason, creates no
backup, and reopens the freshly validated predecessor. Invalid predecessor data
must then be repaired through ordinary predecessor commands before a new
migration operation is attempted.

The stage journal advances atomically with each bounded mutation transaction.
The coordinator scans at most 256 rows per page and writes at most 64 mutations
per batch. Required projections build in fresh generations through the frozen
application frontier. A complete startup-equivalent validation of the private
stage is required before atomic rename and parent-directory synchronization.

Cutover atomically activates the exact successor, installs the predecessor
write fence, stores permanent migration evidence and terminal service audit,
and consumes one administration sequence. It consumes no application sequence
and does not rewrite command history. The automatic backup is retained after
success.

## Restart and rollback behavior

Startup reconciles the checksummed external receipt, protected artifacts,
backup manifest, sibling stage, published target, in-database journal, permanent
migration record, and predecessor-write retirement. It never guesses between
inconsistent evidence.

- Before publication, the predecessor remains authoritative. Recovery resumes
  the exact journaled stage or recreates it from the verified backup.
- At the publication boundary, recovery determines whether the exact
  predecessor or exact successor is installed and continues from that state.
- After publication, the successor is not ready until a fresh complete startup
  validation succeeds.
- If published validation fails, recovery restores and freshly validates the
  exact automatic backup before readiness, then records `FailedRolledBack`.
- A target, receipt, stage, backup, journal, retirement, or permanent-record
  combination outside the closed reconciliation table fails closed.

Migration maintenance owns only the selected database files. Database identity
checks and path confinement prevent a stage, backup, or receipt from being
rebound to a sibling database. The public lifecycle needed to drain one live
database while continuing to serve siblings is part of WP-409.

The durable V1 compatibility vectors live under
`fixtures/migrations/durable/v1`. Protobuf schema hashes and record bounds live
under `fixtures/proto`. Run `./scripts/generate-migration-durable-fixtures
--check` after changing a receipt codec, migration table key, or migration
record encoding.

## Exact identity model

One application release has one canonical successor contract bundle. Each
supported predecessor gets a separate direct migration:

```text
exact parent bundle + .riffm source + canonical successor bundle
    -> canonical MigrationBundleV1
```

A supported parent must have the same lineage and a lower version. It does not
have to be the successor bundle's immediate compilation parent. This permits a
release to support several retained predecessor versions without chaining
migrations or producing several successor identities. Source version numbers
alone are insufficient: the lock pins both endpoint bundle hashes.

At most 32 parent migrations may be retained in one Application Source V3.
Every source path, parent artifact path, parent version, and generated
migration artifact path must be unique.

## Author a Gate A migration

Retain the canonical parent bundle before changing the contract. Add the
successor contract source, bump its version, and declare the exact parent and
migration source in `riffdb.application.json`:

```json
{
  "schema": "riffdb.application-source/v3",
  "contract": {
    "lineage": "TicketDesk",
    "source": "riffdb/contract.riff",
    "version": 2
  },
  "migrations": [
    {
      "parent_bundle": "retained/ticketdesk-v1.riffdb.contract.bundle",
      "source": "riffdb/migrations/ticketdesk-v1-to-v2.riffm"
    }
  ]
}
```

The abbreviated object above shows only migration-related members. Source V3
also requires all normal source members and a Python generation target. See
the canonical fixture at
`fixtures/application-manifests/ticketdesk-migration-v3.json`.

For a required field, provide one deterministic row-local expression:

```riffm
migration TicketDesk from 1 to 2 {
  transform Ticket {
    set normalized_priority = old.priority + 1
  }
}
```

Migration expressions may inspect constants and fields on `old`. They cannot
read another row, access the network or filesystem, observe wall-clock time,
or use ambient randomness. Input size, nesting, declarations, expressions,
steps, output bytes, and retained parents are bounded.

For these Gate A changes, the compiler derives the step and no handwritten
clause is needed:

- add an index to an existing entity;
- add a relationship or unique constraint to existing entities;
- add an entity or aggregate invariant over existing rows; and
- add a projection that must be rebuilt from the frozen frontier.

An empty body is valid when all necessary work is compiler-derived:

```riffm
migration TicketDesk from 1 to 2 {}
```

Missing, duplicate, stale, or unnecessary proofs fail with bounded source-span
diagnostics. Gate B rename, retirement, replacement, and enum-map syntax and
Gate C rekey or ownership acknowledgements are reserved in the V1 grammar but
fail closed until their owning work packages implement them.

## Lock and inspect

For a successor, lock creation asks the selected server for an authorized,
read-only parent-aware candidate preview. It then compiles every declared
migration locally and writes Application Lock V4 plus generated migration
bundles:

```bash
riffdb --config "$HOME/.config/riffdb/client.toml" \
  application lock --write
riffdb application lock --check
riffdb --output json migration plan \
  --application riffdb.application.json \
  --lock riffdb.application.lock.json
```

The plan reports the canonical successor, each supported parent and source
hash, migration bundle identity, stable step categories, step count, and fixed
resource bounds. Planning rechecks every locked file and fails on drift. It
does not contact storage or mutate a database.

Application Lock V4 pins the normal generated artifacts, canonical successor
bundle, exact `.riffm` source hash, retained parent bundle version and hash,
and generated `MigrationBundleV1` hash. Do not edit generated bundles or lock
identities by hand.

Contract migrations are operator-only. They are not generated as application
commands, application-role permissions, MCP tools, or MCP resources.

There is intentionally no supported storage-level invocation or manual file
procedure for starting a migration. Until WP-409 supplies the authorized public
administration path, application authors can lock and inspect artifacts but
cannot request redb migration execution through a supported interface.
