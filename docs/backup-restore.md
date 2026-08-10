# Offline Backup and Restore

RiffDB's POC backup and restore operations are offline, exclusive maintenance
operations. They are exposed through the public gRPC API, Rust client, and
`riffdb` CLI. They are not MCP tools, and neither the CLI nor a client receives
or resolves a server filesystem path.

## Configuration

`riffdbd` requires an absolute `--backup-root` path. The path must be lexically
disjoint from the database file, capability digest keys, and idempotency digest
keys. Only the server joins a checked backup name beneath this root.

The service account needs permission to:

- read and write the backup root;
- create and synchronize `<backup-root>/.maintenance`;
- create immutable named backup directories; and
- close, reopen, and, for restore, replace the configured database target.

Do not place the database or secret key files beneath the backup root.

## Names and Operation IDs

A backup name contains 1 through 64 ASCII bytes. Its first byte is a lowercase
letter or digit. Remaining bytes may also contain `-` and `_`. Dots, path
separators, drive prefixes, whitespace, and Unicode are rejected.

Each public API start request carries a UUIDv7 maintenance operation ID. SDK
callers generate it; the CLI generates it for the operator and returns it in
accepted or uncertain output. Keep that ID until the operation reaches a
terminal state. An API/SDK retry uses:

- a fresh transport request ID;
- the same maintenance operation ID; and
- the exact same operation kind, backup name, and restore confirmation.

Reusing an operation ID with different semantic input fails with a stable input
mismatch and starts no work.

## Create a Backup

```bash
riffdb --config "$HOME/.config/riffdb/client.toml" \
  backup create before-upgrade
```

The CLI prints the maintenance operation ID and the current bounded status.
Poll an accepted operation with:

```bash
riffdb --config "$HOME/.config/riffdb/client.toml" \
  backup operation 01900000-0000-7000-8000-000000000000
```

The server durably creates or resolves the external maintenance receipt before
reporting acceptance. It then stops ordinary admission, drains accepted work
under a fixed deadline, closes the database, publishes the complete immutable
backup, and performs the full startup validation again before returning to
readiness.

An existing named backup is never silently replaced.

## Restore a Backup

Restore to a nonempty target requires the exact destructive confirmation:

```bash
riffdb --config "$HOME/.config/riffdb/client.toml" \
  backup restore before-upgrade \
  --confirm-replace-current-database
```

The flag confirms destructive replacement only. It does not authenticate the
caller, grant authority, satisfy an approval, or prove the backup is safe.

For a healthy current database, RiffDB authenticates and authorizes the caller
before draining. It then validates the selected backup in private staging and
freshly authenticates and authorizes the same bearer against the staged
database. The current authorization result cannot satisfy staged
authorization.

For an empty, unreadable, or corrupt target, the restricted recovery path
validates the staged backup and authorizes only against that staged database.
It does not claim a trustworthy current authorization state.

Publication occurs only after complete artifact checksum, storage structure,
catalog, allocator, capability, audit, and active-contract validation. RiffDB
then performs a fresh validation of the published target before readiness.

### Closed backup inventory

New backups contain exactly four regular files:

```text
database.redb
journal.riffextent
format.riffdb
manifest.riffdb
```

The manifest authenticates the redb checkpoint, the complete fixed-size
durability-journal extent, and the exact pre-open format marker as one unit. It
also declares the inclusive binary format range allowed to restore the
physical artifacts. Restore checks that range before discovering, creating,
locking, staging, or replacing the target. It then checks every artifact
checksum, the journal database identity and selected generation, an empty
suffix at the manifest checkpoint, and the marker identity/fixture binding.
The marker is published last, so startup cannot mistake a partially published
restore for current data.

Unknown files, an absent current artifact, a legacy manifest without a physical
range, stale or mixed identities, and checksum damage fail closed. Historical
two- and three-file backups remain decodable for inspection and recovery with
their source release; the current binary does not infer physical compatibility
from successful decoding. Restore a legacy artifact with its source release
before following `docs/compatibility.md`'s declared upgrade edge.

Extent creation reserves and physically zero-fills its fixed capacity before
the database becomes ready. Insufficient space therefore fails during startup,
backup staging, or restore staging—not after a command has been accepted.

The journal format is local recovery media. Replication and changelog records
continue to derive from published durable frontiers and do not copy journal
bytes.

## Interruption and Recovery

Receipts live beneath:

```text
<backup-root>/.maintenance/
```

This subtree is reserved and cannot be selected by a backup name. A receipt is
versioned, checksummed, bounded, atomically replaced, and parent-directory
synchronized. It is durable operational state, not database metadata or
application history, and it is not included in backup artifacts.

An SDK or direct API caller supplies the maintenance operation ID. After a lost
response or process interruption, that caller retries the exact same start
request with the same operation ID. It must not choose a new ID merely because
the prior response was uncertain.

The CLI start commands generate an operation ID internally and cannot reissue a
start request with a caller-selected ID. An accepted or uncertain CLI result
includes the generated ID. Resolve it by polling:

```bash
riffdb --config "$HOME/.config/riffdb/client.toml" \
  backup operation 01900000-0000-7000-8000-000000000000
```

Do not rerun `backup create` or `backup restore` to resolve CLI uncertainty;
that would generate a different operation identity. Startup validates all
receipts and reconciles incomplete work only from exact receipt, manifest,
inventory, and checksum evidence. It does not infer success from a partial
directory or an ambiguous filesystem result.

Unknown receipt versions, invalid checksums, impossible phase transitions, or
receipt/artifact mismatches fail readiness closed. Operator repair of receipt
bytes is unsupported.

## Restore Rewinds History

The POC preserves the `DatabaseId` stored in the backup. Destructive restore
still rewinds sequence allocators and can reuse destroyed sequence suffixes.

A durable `history_incarnation` fence (ADR-0072) now makes that rewind
detectable. The incarnation is a retained-metadata value bootstrapped to 1 and
bumped only on destructive restore. Backups carry the value when present;
responses that expose commit-sequence-derived positions include the current
incarnation. Sequence-anchored requests may send optional
`observed_history_incarnation`; when present and different from the current
value, the server rejects with `RDB-HISTORY-0101` before any storage work.

Detection residual risk: clients that omit `observed_history_incarnation` keep
working unvalidated and can still silently bind stale observations. Adopt the
field for any long-lived cursor, subscription, or sequence-derived assumption.

A destructive restore removes every observation created after the backup's
included frontier. The removed application and administration sequence suffixes
may later be reused for different records. An idempotency key that existed only
in the removed suffix may be accepted again.

After restore, discard all pre-restore:

- outcome, commit, provenance, event, outbox, projection, and audit locators
  beyond the backup frontier;
- idempotency assumptions about the removed suffix;
- pagination cursors, subscriptions, sessions, and process generations;
- read-after-sequence expectations beyond the backup frontier; and
- capability and authorization facts learned from the removed suffix.

Preserving `DatabaseId` does not make those observations valid. Globally stable
pre-restore locators, point-in-time recovery, online backup, incremental backup,
encrypted backup, and remote object storage are outside the POC.
