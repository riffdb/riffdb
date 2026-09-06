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

For a future reimport, start with portability intent and bind the exact
adapter-owned manifest while the source database is still available:

```bash
riffdb --database ticketdesk export start \
  --lineage TicketDesk \
  --scope whole \
  --entities \
  --portability-manifest riffdb/portability-manifest-v3.json \
  --lease-seconds 3600 \
  --output json
```

The manifest is strictly decoded, checked against the pinned contract bundle,
and retained by hash as part of the operation's immutable retry identity. An
existing general export cannot be replayed with this option or upgraded into
portability authority later.

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

A portability-intent export additionally inspects every selected workflow
row at the same snapshot. Lease owner and expiry must both be null. Each
workflow's checked and quiescent row counts are sealed into the v2 export
manifest; the v2 receipt binds the exact portability-manifest hash. An active
lease or a partially cleared owner/expiry pair closes the export as
`workflow_not_quiescent` before a completed portability receipt exists. Release
or expire the work at the source and start a new export. A general export does
not perform or claim this proof.

## Reimport boundary

JSONL output is not accepted by a generic insert or transaction API. An
application's conformance manifest must map portable record classes to
operator-only `reimport command` declarations or an accepted migration plan.
Reimport therefore re-applies current types, invariants, references, row
policies, provenance, declared outcomes, and server-derived command
idempotency. Commit sequences and physical identities are not portable
identities.

The adapter-owned `riffdb.application-portability-manifest/v3` binds the exact
adapter manifest, contract lineage, contract version, bundle hash, and query
module used by each reconciliation observation. Observation parameters are
canonical typed scalar values in symbolic-name order; field IDs, nested
records, vectors, and caller-supplied query source are not representable. Each
portable entity or event symbol selects only one of these closed strategies:

- a named compiler-owned reimport command whose sole input is a bounded list
  of the exact exported entity record; or
- one exact migration hash already accepted by the adapter's evolution
  manifest.

There is no callback, method path, table name, field ID, transaction handle, or
generic entity writer. The manifest has no idempotency-input or field-mapping
authority: the server derives identity from the exact export and portability
manifests plus the stable entity key. Reconciliation runs declared, bounded
named queries and publishes a canonical
`riffdb.application-reimport-receipt/v2` only when every mapping outcome and
observation digest agrees with the portability manifest. The receipt records
the new database identity and deliberately does not claim preservation of
physical commit sequences.

Frozen v1 and v2 portability manifests and receipts remain readable for
compatibility inspection, but neither older format can be emitted as v3
authority and a v1 application-command mapping cannot be compiled as a modern
reimport command. OpenFGA, MLflow, Better Auth, and Woodpecker compiler
fixtures prove the closed mapping boundary. The MLflow and Woodpecker JSONL
fixtures additionally carry noninitial workflow state, nonzero fencing tokens
and attempts, and null owner/expiry fields; the same parser used by the service
acceptance tests rejects noncanonical or type-inexact variants. Portability-
intent export and its source-side quiescence proof are available through the
public start/page/status/cancel export campaign. A distinct operator-only
destination campaign accepts that exact completed source through
`reimport start`, `reimport page`, `reimport status`, and `reimport cancel`.
It remains unavailable to application roles and MCP discovery.
Each general-alpha subject retains its ADR-0201 evidence classification through
this portability evidence; an export result does not turn a matrix or partial
profile into upstream-framework conformance. The earlier Payload fixture remains
a post-alpha portability regression and is not a general-alpha record.

## Reimport an exact completed source

Reimport targets a new, empty database that has not been published ready. The
credential must carry one Capability V7 reimport grant bound to the campaign
ID, lineage, portability-manifest hash, scope, and compiled application-role
identity. Export authority, installation authority, and an ordinary
application role do not imply this grant.

Start with all three terminal source documents. Reuse `--campaign-id` for
every uncertain retry:

```console
riffdb --database restored --credential-file operator-reimport.credential \
  reimport start \
  --campaign-id 019f... \
  --lineage TicketDesk \
  --scope whole \
  --portability-manifest riffdb/portability-manifest-v3.json \
  --export-manifest export/manifest.json \
  --export-receipt export/receipt.json
```

Apply each entity page in its original order. The page arguments come from
the corresponding `export page` result and must not be recomputed or edited:

```console
riffdb --database restored --credential-file operator-reimport.credential \
  reimport page \
  --campaign-id 019f... \
  --export-operation-id 019e... \
  --page-number 1 \
  --jsonl export/entities-0001.jsonl \
  --page-hash 7d6f... \
  --next-cursor RF...
```

For a terminal page, omit `--next-cursor` and pass both
`--class-complete --operation-complete`. RiffDB verifies the domain-separated
page hash, exact expected page position, source manifest, current authority,
and every canonical row before compiler-owned commands can mutate the empty
destination. A repeated page is an exact replay; a different page at the same
position fails closed. The final page is also the recovery entry point if its
commands committed before reconciliation completed: RiffDB replays only
commands not covered by the durable page checkpoint, resumes the exact
module-bound observations, and returns the already sealed receipt after an
uncertain terminal response.

Observe or cancel with the same campaign identity:

```console
riffdb --database restored reimport status --campaign-id 019f...
riffdb --database restored reimport cancel --campaign-id 019f...
```

Cancellation and partial failure never publish the destination ready. After
all pages, the coordinator performs the portability manifest's bounded
observations and publishes a canonical reimport receipt only when every
mapping and observation reconciles. The installation campaign may advance to
credentials only after that receipt is retained. There is no CLI verb that
accepts raw JSONL without its export operation, page number, page hash,
completion flags, and cursor evidence.
