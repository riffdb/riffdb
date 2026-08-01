# Contract migrations

RiffDB distinguishes a compatible contract successor from one that requires
existing authoritative rows or derived state to be checked or transformed.
The latter is `RequiresMigration`: the candidate is valid, but deployment does
not activate it without an exact migration artifact for the active parent.

The current implementation compiles and locks Gate A migration artifacts and
can inspect them locally. WP-407 also provides the deterministic memory
reference model for complete preflight, bounded row/index batches, projection
candidate readiness, and final cutover. It does **not** yet apply a migration to
a redb database or expose migration through the server. `riffdb migration plan`
remains read-only. Durable staging and crash recovery arrive in WP-408; public
authorization and administration arrive in WP-409; WP-410 proves the installed
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

These semantics are not a hidden storage-edit API. The memory stage is a test
and conformance model; production redb staging, durable journal/record codecs,
backup identity, publication, and restart reconciliation belong to WP-408.

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
