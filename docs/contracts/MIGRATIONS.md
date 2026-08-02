# Contract migrations

The complete P7 operational and compatibility evidence is indexed in
[Contract Migration Acceptance](../operations/CONTRACT-MIGRATION-ACCEPTANCE.md).

RiffDB distinguishes a compatible contract successor from one that requires
existing authoritative rows or derived state to be checked or transformed.
The latter is `RequiresMigration`: the candidate is valid, but deployment does
not activate it without an exact migration artifact for the active parent.

The current implementation compiles and locks Gate A additive, Gate B
structural, and Gate C key/ownership migration artifacts, checks and applies them through the public
administration service, and drives
the internal redb execution and startup-recovery path: complete
preflight, an immutable automatic backup, bounded staged transforms, projection
rebuild, complete validation, atomic publication, and automatic
post-publication rollback. `riffdb migration plan` remains a local read-only
inspection. The retained TicketDesk Gate-A, StructuralRows Gate-B, and
KeyOwnershipRows Gate-C fixtures freeze the additive, structural, and
key/ownership artifact boundaries.

## Semantic execution boundary

The catalog rechecks the exact parent, candidate, migration hash, direct
compatibility, and complete gate-specific proof before producing a validated plan.
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

Every transformed row archives its exact displaced predecessor envelope under
the migration operation identity in the successor database. Startup accepts a
version transition without an application commit only when that archive row,
the permanent migration record, the migration hash, the predecessor chain tip,
and the successor schema binding all agree. Missing or inconsistent evidence
fails closed. These archives preserve historical continuity; they are not a
second writable entity store.

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
rebound to a sibling database. Apply drains only the database selected by the
client's `--database` or client configuration. Other configured databases keep
serving, while one process-wide migration exclusion prevents a second check or
apply from starting concurrently.

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

## Author a migration

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
diagnostics.

### Structural Gate B

A semantic rename keeps the original numeric stable ID only when the exact type
and semantic role remain unchanged. The predecessor name becomes a permanent
lineage alias and can never be allocated again. Use the fully qualified owner
for scoped identities:

```riffm
migration StructuralRows from 1 to 2 {
  rename entity Row to Record
  rename field Row.status to workflow_status
}
```

Renaming an entity, field, enum variant, command, event, outcome, or projection
uses the migration-aware successor compiler. Ordinary successor compilation
does not infer a rename and continues to reject stable-ID reuse. The generated
application lock pins the migration-aware successor, so lock checks and deploy
preview agree on one exact bundle identity.

Changing a field's type or meaning is a replacement, not a rename. The old
identity is tombstoned, the replacement receives a fresh field ID, and every
predecessor row is checked before staging begins:

```riffm
migration StructuralRows from 1 to 2 {
  transform Row {
    require old.value >= 0
    replace value with amount using checked_i64_to_u64
  }
}
```

The closed Gate-B conversion names are:

- `identity` and `wrap_optional`;
- `assert_unwrap_optional`;
- `checked_i64_to_u64` and `checked_u64_to_i64`;
- `decimal_exact`;
- `assert_bounded_narrow` for strings, bytes, and lists;
- `list_elements` for the closed element-conversion set; and
- `uuid_to_string` and `string_to_uuid` using lowercase hyphenated UUID text.

The complete migration fails preflight on a failed `require`, null optional
unwrap, integer overflow or sign error, inexact decimal rescale, bound
violation, invalid list element, or noncanonical UUID string. It never rounds,
truncates, clamps, substitutes a fallback, or skips a row.

Enum changes require one exhaustive mapping entry for every predecessor
variant. Mapping targets must exist in the successor enum:

```riffm
map enum WorkflowStatus {
  Open -> Open
  Closed -> Archived
}
```

`retire` logically removes an entity, command, event, outcome, enum variant, or
projection identity. Entity rows move to the migration archive; historical
bundles and committed events, outcomes, provenance, and idempotency evidence
remain byte-exact. Retiring a command or entity also closes its owned identity
tree. There is no physical purge. A replacement projection uses a fresh
projection identity; retire the old projection and let the compiler derive the
new projection rebuild through the frozen frontier.

```riffm
migration Surface from 4 to 5 {
  retire command LegacyWrite
  retire projection OldTotals
}
```

Gate C supports primary-key replacement, rekeying, repartitioning,
aggregate-membership changes, relationship-target changes, and conflict-domain
changes. A rekey supplies every successor primary-key component as a row-local
expression over the complete predecessor row:

```riffm
migration Accounts from 2 to 3 {
  transform Account {
    set region = "global"
    rekey (old.tenant_id, "global", old.account_id)
  }
  acknowledge repartition Accounts
  acknowledge conflict Accounts
}
```

The compiler derives all affected authoritative indexes, relationships,
uniqueness checks, aggregate ownership, partition materialization, and conflict
meaning from the exact parent and successor. An acknowledgement authorizes only
the corresponding compiler-derived aggregate change; it cannot supply a key or
override the candidate schema.

Preflight evaluates every row before backup, proves complete successor-target
uniqueness, resolves relationships against the complete candidate target set,
and validates successor unique prefixes. A target already occupied in the
predecessor keyspace fails closed, even when another row would later vacate it.
Consequently Gate C does not support key swaps or predecessor-occupied rekey
chains; those require a future two-phase key namespace rather than inferred
ordering. Each admitted move archives the exact old target and row, advances the
entity version once, removes every old index suffix, and installs the successor
row and complete index set in the same journaled stage transaction. The private
stage remains invisible until final validation and cutover retire predecessor
writes.

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

## Authorize a migration operator

Migration authority is a dedicated global, database/environment-bound, exact
lineage permission. Deploy, backup, restore, capability administration, MCP,
and application-role authority do not imply it. Create a separate short-lived
operator capability through an existing capability administrator:

```json
{
  "principal_id": "migration-operator",
  "actor_kind": "human",
  "requested_lifetime_seconds": 3600,
  "audiences": ["riffdb-grpc-loopback"],
  "grant": {
    "tenant_scope": { "type": "global" },
    "partition_scope": { "type": "all" },
    "permissions": [
      { "type": "migrate_contract", "contract_lineage": "TicketDesk" }
    ],
    "field_visibility": [],
    "max_scan_rows": 1,
    "approval_required": []
  }
}
```

```bash
riffdb --database ticketdesk \
  --credential-file "$HOME/.config/riffdb/operator.credential" \
  capability create \
  --request migration-operator.json \
  --credential-output "$HOME/.config/riffdb/migration.credential"
```

Adding `"migrate_contract"` to `approval_required` denies migration until an
approval integration can satisfy that obligation. The POC does not provide
such an integration, so leave it absent for an intentionally authorized local
operator. Retain the returned capability ID so it can be revoked.

## Check, apply, and observe

First inspect the lock and retain the exact lowercase migration hash:

```bash
riffdb migration plan \
  --application riffdb.application.json \
  --lock riffdb.application.lock.json
```

Use a fresh canonical UUIDv7 for check. A lock with multiple retained parents
also requires `--migration-hash`; omitting it is accepted only when exactly one
migration is locked.

```bash
riffdb --database ticketdesk \
  --credential-file "$HOME/.config/riffdb/migration.credential" \
  migration check \
  --operation-id "$check_operation_uuidv7" \
  --migration-hash "$migration_hash"
```

Check is durable and read-only. Retry an uncertain check with the same
operation ID, artifacts, and migration hash. Apply is a different semantic
operation and therefore requires a different UUIDv7. Its confirmation is both
the exact selector and the destructive confirmation:

```bash
riffdb --database ticketdesk \
  --credential-file "$HOME/.config/riffdb/migration.credential" \
  migration apply \
  --operation-id "$apply_operation_uuidv7" \
  --confirm-apply "$migration_hash"
```

After durable acceptance, client cancellation does not cancel apply. The
selected database rejects ordinary traffic while draining, offline, staging,
publishing, and validating; configured siblings remain available. Poll after
the selected database reopens:

```bash
riffdb --database ticketdesk \
  --credential-file "$HOME/.config/riffdb/migration.credential" \
  migration operation "$apply_operation_uuidv7"
```

Responses contain only the closed phase/failure classes, exact artifact hashes,
and protected backup identity. They never contain row values,
credentials, absolute paths, or internal error sources. Same operation ID with
different input fails before drain. A successful repeat apply can resolve as
`already_applied` only when the permanent migration edge and retained terminal
receipt match exactly.

There remains no supported storage-level or manual file procedure for starting
a migration. TypeScript, Python, generated application clients, and MCP expose
no migration authority or operation; use the CLI, public gRPC administration
API, or the stable Rust client facade.
