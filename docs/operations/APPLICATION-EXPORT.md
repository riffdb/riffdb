# Symbolic Application Export

RiffDB application export is an operator-authorized portability surface. It
reads one immutable published snapshot and releases bounded canonical JSONL
pages containing symbolic contract names and natural typed values. It never
exports redb keys, numeric schema IDs, durable envelopes, journal/changelog
bytes, credentials, or hidden schema.

Export is not backup. Use [Backup and Restore](../backup-restore.md) for an
exact disaster-recovery copy within its declared durable-format range. Use
application export when data must cross an incompatible alpha format or leave
RiffDB through a portable application representation.

## Authority and scopes

An export requires an explicit Capability V5 grant for the selected contract
lineage and record classes. Entity or event read permission, capability
administration, backup authority, and application query roles do not imply
export authority.

The two scopes are:

- `principal`: applies the current application role's row policy and field
  visibility before serialization, counting, cursor advancement, and hashing;
- `whole`: requires a distinct whole-application grant with global tenant and
  all-partition scope.

Provenance and public audit each require their own grant bit. V5 deliberately
does not permit audit-only or provenance-only export: at least entities or
events must also be selected.

Every start, page, status, and cancel operation reloads the exact current
capability. Revision, revocation, role, field-visibility, or row-policy changes
close an in-progress export rather than continuing under stale authority.

## Start an immutable export

The CLI generates a UUIDv7 operation identity unless `--operation-id` is
provided. Supplying a stable operation identity makes an uncertain start safe
to retry.

```bash
riffdb --database ticketdesk export start \
  --lineage TicketDesk \
  --scope whole \
  --entities \
  --events \
  --provenance \
  --lease-seconds 3600 \
  --output json
```

The response contains the operation identity, exact snapshot binding, and an
opaque base64 cursor. The cursor is bound to the operation, snapshot,
principal, scope, and export format. Do not decode or edit it.

## Write bounded JSONL pages

Pass the returned cursor to `export page` and select a new local output file:

```bash
riffdb --database ticketdesk export page \
  --operation-id 018f2f85-3c20-7a31-8f11-112233445566 \
  --cursor 'OPAQUE_BASE64_CURSOR' \
  --max-rows 500 \
  --jsonl export/entities-0001.jsonl \
  --output json
```

The destination must not already exist. RiffDB creates it with mode `0600`,
writes one canonical JSON object per line, flushes it, and reports the page
hash, class completion, operation completion, and next cursor. Use a distinct
file for every page. A response lost after the server advances its durable
checkpoint can be retried with the same cursor during the lease; the server
replays only the exact page bound to that durable successor.

Pages are bounded to 500 rows, four MiB of canonical JSON, and the operation's
server time budget. The client never accumulates the complete export in
memory.

## Observe or cancel

```bash
riffdb --database ticketdesk export status \
  --operation-id 018f2f85-3c20-7a31-8f11-112233445566

riffdb --database ticketdesk export cancel \
  --operation-id 018f2f85-3c20-7a31-8f11-112233445566
```

A completed operation includes a canonical manifest and receipt binding the
database/history identity, contract and module identities, row-policy scope,
snapshot frontiers, per-class and total counts and bytes, ordered page hashes,
omissions, capability identity/revision, and terminal state. Cancellation,
expiry, authority change, source failure, and process crash produce or retain
an inspectable incomplete operation; they are never reported as complete.

An immutable redb read transaction is process-local. After a process crash,
the durable checkpoint remains observable, but its old snapshot cannot be
continued. RiffDB returns a typed snapshot-unavailable/restart outcome rather
than silently selecting a newer snapshot. Start a new operation and retain the
older incomplete receipt.

## Reimport boundary

JSONL output is not accepted by a generic insert or transaction API. An
application's conformance manifest must map portable record classes to
declared idempotent compiled commands or an accepted migration plan. Reimport
therefore re-applies current types, invariants, references, row policies,
provenance, declared outcomes, and command idempotency. Commit sequences and
physical identities are not portable identities.

The adapter-owned `riffdb.application-portability-manifest/v1` binds the exact
adapter manifest, contract lineage, contract version, and bundle hash. Each
portable entity or event symbol selects only one of these closed strategies:

- a named compiled command with exhaustive symbolic field inputs; or
- a named bounded-list command whose record element is the exact exported
  entity/event record; or
- one exact migration hash already accepted by the adapter's evolution
  manifest.

There is no callback, method path, table name, field ID, transaction handle, or
generic entity writer. Command idempotency input is compiler-checked and the
reimport runner derives its value from the exported stable record identity; it
is not caller-selected data from the export. Reconciliation runs declared,
bounded named queries and publishes a canonical
`riffdb.application-reimport-receipt/v1` only when every mapping outcome and
observation digest agrees with the portability manifest. The receipt records
the new database identity and deliberately does not claim preservation of
physical commit sequences.

Current POC limitation: OpenFGA- and Payload-shaped mappings compile and have
canonical reconciliation fixtures. The all-adapter release claim remains
blocked for the MLflow and Woodpecker workflow shapes. An ordinary command
cannot directly initialize a protected workflow-state field, which is the safe
failure; RiffDB does not yet have an accepted compiler-owned reconstitution
operation that can restore workflow state and lease evidence without creating
a normal state-write bypass. Until that design is accepted and implemented,
the four-domain `EXP-014` gate is intentionally incomplete.
