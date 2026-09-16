# Offline Backup and Restore

RiffDB's POC backup and restore operations are offline, exclusive maintenance
operations. They are exposed through the public gRPC API, Rust client, and
`riffdb` CLI. They are not MCP tools, and neither the CLI nor a client receives
or resolves a server filesystem path.

## Configuration

WP-772 implementation note: for an already-active changelog V3 database, the
offline incarnation stamp now validates retained receipts and source fences
before atomically installing a new lineage anchor and clearing replication-local
progress. A destructive restore cannot continue the old replication tail; a
receiver needs a new bootstrap. This internal substrate is still under
integration and does not announce production replication availability. The
public maintenance operation ID and retry protocol below are unchanged. See the
[V3 substrate notes](architecture/CHANGELOG-V3.md) for the remaining limits.

The offline retention-watermark stamp also validates retained V3 history before
returning from an equal-value retry. A changed watermark and its exact V3 receipt
commit together in the existing hardened transaction. This adds no commit or
flush and changes neither watermark normalization nor the recorded tombstone
chain-root binding.

Offline retention also respects each registered follower's durable acknowledged
application frontier. A follower that has acknowledged no application commit
pins the watermark at zero; a faster follower cannot release a slower follower's
fence. Retention status reports `FollowerLowWater` when this is the binding
minimum. Unreadable or inconsistent V3 hold evidence refuses collection and
prune. Lag does not expire a hold automatically. The administrative retirement,
configured expiry, hold-budget health and promotion ceremonies remain WP-748 work.

`riffdbd` requires an absolute `--backup-root` path. The path must be lexically
disjoint from the database file, capability digest keys, and idempotency digest
keys. Only the server joins a checked backup name beneath this root.

The service account needs permission to:

- read and write the backup root;
- create and synchronize `<backup-root>/.maintenance`;
- create and retire immutable named backup directories; and
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

## Remote container disaster drill

The release Compose topology can exercise total database-volume loss through
the same public maintenance API used by operators:

```bash
./scripts/remote-compose-acceptance --backup-restore
```

The drill deploys and seeds the release-owned four-subject workload corpus,
creates and polls a backup from the separate operator container, creates a
post-backup authority record, stops the database container, empties only its
freshly allocated test data root, starts an empty replacement, and restores the
backup through the public TLS endpoint. It then reruns the generated OpenFGA,
MLflow, Better Auth, and Woodpecker observations, including a typed Better Auth
user/session graph whose secret token is never placed in the observation or
receipt. The retained Payload operational-query shape runs in the same corpus
as a post-alpha regression and is labeled separately. The drill proves that the
backed-up operator and application authorities work and that the post-backup
authority disappeared. The operator container receives no database or backup
mount and never supplies a server filesystem path.

The complete adapter gate also validates each domain's immutable conformance
manifest and installation/evolution plans before running that destructive
corpus:

```bash
./scripts/adapter-disaster-recovery-acceptance --all-domains --remote
```

The drill writes a secret-free JSON receipt beneath
`target/adapter-disaster-recovery/`. The receipt binds both maintenance
operation identities and input hashes, the verified backup-manifest checksum,
the destroyed post-backup suffix, the exact contract bundle, startup
validation, the accepted four-subject inventory, the separately classified
Payload regression, and the reconciled adapter-observation digest. The backup
manifest is the frontier-bearing durable artifact; the receipt binds its
checksum rather than decoding storage-format bytes in application or operator
code. Recovery proves the declared lifecycle observation and does not promote any subject's evidence classification.
In particular, the MLflow and Woodpecker matrix results do not become upstream-
framework conformance through backup and restore.

An existing named backup is never silently replaced.

## Retire a Backup

Published immutable backups must not be deleted directly. Retire one exact
checked name through the authorized public surface:

```bash
riffdb --config "$HOME/.config/riffdb/client.toml" \
  backup retire before-upgrade
```

Retirement requires the same global `AdministerCapabilities` authority as
create and restore. The server resolves the exact succeeded create receipt and
manifest; the client never submits a path. It then renames the complete
four-file inventory into an operation-private maintenance directory, syncs both
parents, deletes only those four known regular files, and records terminal
success. Poll the returned operation ID with `backup operation` exactly as for
create and restore.

A terminal retirement receipt is the only reason startup accepts a succeeded
create receipt whose published artifact is absent. Missing artifacts without
that exact receipt pair, symlinks, unknown files, manifest mismatches, and
ambiguous staged state fail readiness closed. Retired names remain permanently
consumed: neither create nor restore can silently reuse them.

Do not remove a backup directory or any file beneath `.maintenance` with
filesystem tools. Routine retention should invoke `backup retire` after a newer
backup has been fully validated. An interrupted retirement is recovered from
its durable phase and exact create/manifest evidence on restart.

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

WP-749 implementation status: the external ledger also recognizes archive-only
receipt V3. It preserves the verified archive selection across retries, shares
the existing maintenance ownership and inventory limit, and retains an admitted
private replay stage for archive recovery. Private replay can stop at the receipt's
exact selected manifest after the archive advances; an explicitly empty selected
suffix remains empty. Ordinary create/restore V1 and retire
V2 receipt bytes are unchanged. An unfinished V3 receipt currently refuses server
startup; daemon recovery routing and the public restore command are still being
implemented. Recognizing a receipt does not validate or publish its replay stage.
For an interior command stop, storage can rebuild a separate private artifact
from the verified backup and checked command-prefix evidence after full original
replay. It retains the original archive selection separately from its actual
application/audit frontier. Ordinary source and follower opens refuse this
artifact. A separate validation owner now checks its exact construction bytes,
full structural/catalog evidence, reciprocal command graph and actual stopped
frontier. It can release one pinned snapshot for staged authentication and current
policy without local writes. Once every authorization reader closes, storage can
seal the validated cut and publish it through the matching durable Offline V3
receipt. Its recorded incarnation authorizes a fresh RestoreAnchor at the actual
stopped frontier; only then is the source journal created and the normal format
marker restored. Interrupted private stages rebuild from the receipt's original selection.
One storage preparation owner now covers an empty suffix, the exact backup
fence, earlier complete receipts and interior command cuts. It first validates
the entire selected original suffix, then rebuilds an earlier stop from the same
backup and selection. An explicit sequence stops at its first exact boundary;
last-archived retains the selected terminal administration frontier. Both paths
provide a validated immutable snapshot for staged authorization and use the
same receipt-authorized publication owner. The internal driver now drains an
admitted V3 receipt, resolves its existing owned archive from operator configuration,
and persists
its verified selection before replay, authorizes the validated immutable stage,
and records the actual frontier and incarnation before publication. Success
requires fresh source startup validation and complete authoritative state equality
with the retained stage. Before publication is durably recorded, credential-free
resumption requires exact staged/target bytes and the receipt-bound RestoreAnchor,
marker and empty journal. After publication is recorded, recovery permits engine
bookkeeping changes from interrupted startup, but requires the same lineage, RestoreAnchor and dual frontier, permits only
empty `DirtyActivation` receipts from those startups, and compares every
authoritative row against the retained published stage before success.
It cannot rebuild an unpublished stage without credentials.
The storage owner can publish a separately validated and sealed replay only against
matching durable Offline V3 evidence and its recorded incarnation. It creates an
empty source journal at the actual restored application/administration frontier,
validates the new source completely, and then uses the existing replacement and
parent-sync boundaries. Pre-receipt cleanup cannot delete an admitted V3 stage.
The shared staged-authorization check uses one immutable restored-state snapshot;
it can check replayed grants, expiry, approvals and revocations without local
follower authority writes.

### Archive restore integration status

A ready source server now accepts the distinct `RestoreArchivedBackup` RPC and
Rust-client `RestoreArchivedBackup` submission. The CLI uses the same service:

```bash
riffdb --config "$HOME/.config/riffdb/client.toml" \
  storage restore before-upgrade --archive daily \
  --stop-at-sequence 42 --confirm-replace-current-database
```

The archive name resolves through [server configuration](configuration.md).
Omitting `--stop-at-sequence` selects the last archived frontier when the server
first verifies the archive. An explicit sequence must be positive and fall
within the verified backup and selected suffix. The operation freezes that
selection before replay, and a retry retains the operation ID, archive name,
stop choice and replacement confirmation. The client never falls back to an
ordinary restore when the archive RPC is unavailable.

Accepted and uncertain output contains the maintenance operation ID. Poll it
using `backup operation`; rerunning `storage restore` generates a new ID.
Archive status includes `archive_restore.archive_name`, the requested stop,
and, once resolved, `backup_application_frontier` and `restored_frontier`.
The latter has independent application and administration positions. An absent
frontier means unresolved; `before_first` means a known empty history. Ordinary
maintenance output is unchanged.

This remains incomplete WP-749 integration. Automatic archive collection,
daemon restart routing for an unfinished V3 receipt, and source-less archive
restore admission are still unavailable. Restart with an unfinished V3 receipt
refuses readiness; the internal recovery driver proofs do not yet provide a
public restart recovery ceremony. Do not rely on this increment for disaster
recovery. Receipt editing is unsupported.

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

Do not rerun `backup create`, `backup restore`, or `backup retire` to resolve CLI uncertainty;
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

A backup may physically contain the source database's private clean-close
lifecycle record, but restore never treats that record as permission to skip
validation. Destructive restore changes the history incarnation and staged
restore performs complete validation; the lifecycle is therefore stale and is
consumed or replaced as dirty before the restored database can activate
writers. A later graceful shutdown may produce a new clean record bound to the
restored incarnation. Copying backup artifacts or a database file cannot forge
clean eligibility.

The immutable backup artifact preserves the source bytes, including a matching
certificate. Private restore staging nevertheless drives the complete
structural and catalog-semantic streams through exact end before staged
authentication; it never uses those bytes to skip restore validation. Because
that sealed scrub is read-only, the later ordinary open used solely for staged
authentication may still report the matching clean mode. Eligibility is a
property of the complete stopped database, journal, format marker, maintenance
state, and retained roots, not of certificate bytes alone. A complete stopped
lifecycle-unit copy preserves eligibility; a bare database-file copy does not
and takes complete validation. Registry migration, an incompatible internal
format, maintenance replacement, retention hold or prune, and
history-incarnation rotation likewise invalidate or fail closed. A backup-bound
compatible marker-only format upgrade preserves an otherwise unchanged
lifecycle unit. An offline integrity scrub deliberately preserves the bytes but
does not consume, refresh, or replace them.

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
