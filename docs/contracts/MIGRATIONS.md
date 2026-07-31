# Contract migrations

RiffDB distinguishes a compatible contract successor from one that requires
existing authoritative rows or derived state to be checked or transformed.
The latter is `RequiresMigration`: the candidate is valid, but deployment does
not activate it without an exact migration artifact for the active parent.

The current implementation compiles and locks Gate A migration artifacts and
can inspect them locally. It does **not** yet execute a migration or cut over an
active database. `riffdb migration plan` is read-only. Runtime application,
crash recovery, authorization, and cutover arrive in WP-407 through WP-410.

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
