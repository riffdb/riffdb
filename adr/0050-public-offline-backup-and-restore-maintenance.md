# ADR-0050: Public Offline Backup and Restore Maintenance

- **Status:** Accepted
- **Direction approved:** 2026-07-24
- **Exact text accepted:** 2026-07-24
- **Accepted:** 2026-07-24
- **Acceptance reference:** Human maintainer confirmation in the current Codex
  session on 2026-07-24
- **Clarification accepted:** 2026-07-24, human maintainer confirmation in the
  current Codex session
- **Requires:** ADR-0004, ADR-0005, ADR-0006, ADR-0007, ADR-0009,
  ADR-0018, ADR-0019, ADR-0021, ADR-0025, ADR-0026, ADR-0027,
  ADR-0028, ADR-0032, ADR-0034, ADR-0037, ADR-0040, ADR-0041, and
  ADR-0049
- **Resolves:** ADR-0041's reserved WP-155 public backup/restore boundary
- **Amends:** SPEC Sections 5.2 through 5.3, 10.2, 10.6, 11.2, 13.3
  through 13.5, 16.1 through 16.3, 17.5 through 17.7, 18.5 through
  18.6, 19.2 through 19.5, 20.3 through 20.4, 21, 22, Appendices B and
  C, and the affected WP-070, WP-120, WP-127, WP-130, WP-150,
  WP-185, WP-190, and WP-200 package boundaries
- **Decision deadline:** Before WP-155 implementation begins and before
  WP-190 or WP-200 can complete

The human maintainer accepted this exact design in the current Codex session
on 2026-07-24. This record activates the reserved WP-155 package and makes it a
P2 gate member after WP-185 and before WP-190. It reuses WP-070's existing
offline redb backup manifest and artifact mechanics; it does not create a
second backup format or a privileged CLI file-copy path.

## Context

The POC specification requires an offline consistent backup and verified
restore, but ADR-0041 deliberately left the public operation, authorization,
audit, quiescence, destructive confirmation, uncertainty recovery, and
readiness boundaries unresolved.

An offline operation cannot use the ordinary durable service-audit terminal:
the database is closed during backup and may be replaced during restore.
Appending a terminal after restore would mutate the restored historical
snapshot and could allocate an administration sequence that did not exist in
the backup. A process crash can also occur after filesystem publication but
before a public response. A narrow receipt outside the database is therefore
required for maintenance audit and uncertainty recovery.

Restore creates a second semantic hazard. The POC has a permanent
`DatabaseId` but no database-incarnation or history-epoch identity. Restoring
an older snapshot preserves `DatabaseId`, removes a suffix of application and
administration history, and permits the restored allocators to reuse those
sequence values. That limitation must be explicit rather than hidden behind a
claim that old locators remain globally unique.

## Decision

### Add exactly three public operations

The existing public `AdminService` gains exactly these additive unary RPCs:

```text
CreateOfflineBackup
RestoreOfflineBackup
GetOfflineMaintenanceOperation
```

The public RPC inventory becomes five services and 25 RPCs. These operations
are available through gRPC, `riffdb-client-rust`, and `riffdb-cli`. They are not
MCP tools or resources, are not added to the MCP fixed-tool registry, and are
not available through `riffdb-mcp`.

The exact CLI identities and grammar roots are:

```text
backup.create     riffdb backup create <name>
backup.restore    riffdb backup restore <name> [--confirm-replace-current-database]
backup.operation  riffdb backup operation <maintenance-operation-id>
```

Machine output retains the existing `riffdb.cli.output/v1` envelope and owns
closed command-specific DTOs and goldens. No command opens redb or resolves a
filesystem path locally.

Every start request carries:

- a fresh checked transport `RequestId`;
- a caller-stable checked UUIDv7 `OfflineMaintenanceOperationId`;
- the exact operation kind;
- one checked `BackupNameV1`; and
- for restore, the closed replacement confirmation.

`BackupNameV1` contains 1 through 64 ASCII bytes. The first byte is lowercase
`a` through `z` or `0` through `9`; remaining bytes may additionally be `-` or
`_`. It contains no dot, separator, drive prefix, normalization, Unicode, or
empty component. The server joins only this value beneath its configured
absolute `backup_root`. A public request never carries a database path, backup
root, artifact path, receipt path, URI, or arbitrary filename.

The restore wire confirmation is the exact closed value
`ALLOW_REPLACE_NONEMPTY_TARGET`. The CLI emits it only for
`--confirm-replace-current-database`. Every nonempty target, including a
healthy, unreadable, or corrupt target, requires that value. An empty target
does not require another flag. Confirmation is not authorization, approval,
authentication, or evidence that the target is safe to replace.

Starting an operation durably creates or resolves its receipt before returning
`Accepted`, `AlreadyAccepted`, or a terminal observation. Reusing an
`OfflineMaintenanceOperationId` with a different operation kind, backup name,
or confirmation is a stable input-mismatch failure and starts no work. Each
retry uses a fresh `RequestId` and the same maintenance operation ID and exact
semantic input. `GetOfflineMaintenanceOperation` returns only a bounded closed
status and safe failure class.

WP-155 is a schema-owner phase solely for these three operations, their
messages, the new public maintenance identifier, and their exact client/CLI
fixtures. It may not change an existing RPC, field number, service, durable
message, MCP schema, contract bundle, or command protocol.

### Use the shared service and policy without extending durable audit enums

`riffdb-service` owns API-neutral maintenance request/result traits and the
orchestration entry point. gRPC performs only structural decoding,
authentication handoff, bounds, and total wire conversion. The SDK and CLI use
only the public gRPC client. No adapter calls storage-redb directly.

`riffdb-policy` owns a narrow offline-maintenance authorization request and
proof. It reuses current capability resolution, expiry, environment, audience,
tenant, approval, and deny-by-default behavior. POC maintenance requires the
existing unparameterized `AdministerCapabilities` permission, global tenant
scope, and every ordinarily applicable approval obligation. It does not add a
new durable capability-permission tag. The maintenance operation kind is a
separate process-local closed registry and does not extend the 22-value durable
`ServiceOperationV1` registry.

The maintenance receipt is the accepted audit exception. WP-155 does not append
`StoredServiceAuditRecordV1` Started or terminal rows for these operations and
does not synthesize a terminal after reopen. This preserves the accepted
22-operation durable audit enum, exact durable registry, restored snapshot, and
administration allocator. The receipt records the bounded admitted principal,
operation, input identity, approval disposition, and terminal class needed for
audit and recovery. Bootstrap remains the sole principal-less database
mutation; restore of an empty or corrupt target is separately authorized
against the staged backup and is never principal-less.

### Retain one credential only across the restore handoff

`riffdb-auth` owns one non-`Clone`, nonserializable, redacted, bounded,
zeroizing maintenance credential handoff. It accepts only the already
bearer-stripped ordinary 43-byte presentation and exposes it solely as a
borrowed `OpaqueCredential` to `CredentialAuthenticator`. It has no parser,
digest provider, authorization decision, raw accessor, fallback, or durable
representation.

For replacement of a healthy current database:

1. the ordinary bearer is authenticated and authorized against the current
   database before admission or drain;
2. the exact presentation is retained in the auth-owned handoff;
3. after the selected backup is restored to private staging and completely
   validated, the bearer is freshly authenticated and authorized against that
   staged backup; and
4. only then may publication replace the current target.

The current authorization result cannot satisfy staged authorization. The same
presentation must independently pass both current and staged checks; no
principal, decision, capability fact, or policy result is carried across.

For an empty, unreadable, or corrupt target there is no claimed current
authorization. The selected backup is first checksum-verified, restored to
private staging, and passed through the complete storage-structural, catalog,
digest-inventory, allocator, capability/audit, and active-contract validation
path. The retained bearer is then authenticated and authorized against that
validated staged database. Failure at either stage publishes nothing and
returns only a generic bounded public class.

The credential is never written to a receipt, manifest, database, diagnostic,
trace, metric, or public result. It is dropped before target publication and
before a terminal receipt is persisted. A process crash cannot recover the
credential; the caller retries the same operation ID and presentation, and the
receipt/filesystem state determines whether work may resume.

If that crash occurs after a healthy target admitted and drained a restore but
before destructive publication, startup validates the unchanged current
database but must not expose ordinary readiness. It may expose only the normal
gRPC `RestoreOfflineBackup` admission for the exact operation ID and immutable
semantic input already frozen in the nonterminal receipt. That retry performs a
fresh current-database authentication and authorization using the caller's new
presentation, reacquires the existing operation, and repeats the independent
staged authentication and authorization before publication. An unrelated
operation ID, input mismatch, backup creation, maintenance polling, command,
read, capability administration, bootstrap, health detail, and MCP request all
remain closed.

This credential-recovery admission is a private lifecycle state. It adds no
public operation, durable phase, receipt field, authorization proof, or
credential representation. Exact receipt resolution remains the authority for
retry identity; the presence of a healthy current database does not permit a
new maintenance operation to bypass the incomplete receipt.

### Keep maintenance lifecycle private and exclusive

The server-private lifecycle is:

```text
Ready -> Draining -> Offline -> Validating -> Ready
                                      \----> FailedClosed
```

A startup that cannot validate an empty/corrupt target may expose only
restricted health and the exact recovery-mode `RestoreOfflineBackup` admission
needed to stage and authorize a named backup. It exposes no command, read,
MCP, capability administration, backup-create, or maintenance-polling path.

Admission freezes one operation receipt and closes new ordinary service and MCP
work. Draining waits under a fixed deadline for already accepted service,
coordinator, projection, outbox, notification, and MCP work, then closes their
ports and the redb database. Timeout moves to `FailedClosed`; it never kills a
task and continues as if quiescence were proved.

While redb is offline:

- no protected request is accepted;
- `GetOfflineMaintenanceOperation` is unavailable;
- no bearer is newly authenticated;
- no service audit, command, read, health detail, MCP call, projection query,
  outbox transition, or maintenance start is executed; and
- the one admitted maintenance driver owns the private backend handle.

Backup creation runs WP-070's complete offline artifact publication, then
reopens the unchanged database and repeats the complete authoritative/catalog
startup validation before readiness.

Restore first materializes a private staged target from the immutable named
backup and verifies its complete manifest/artifact checksum. It runs the full
ordinary startup validation and staged authorization against that exact
materialization. It then invokes the backend's source/target-bound destructive
publication under `ALLOW_REPLACE_NONEMPTY_TARGET` when required, re-verifies
the immutable source and target, and runs a fresh complete post-publication
startup validation. Readiness is false until the post-publication proof and
terminal receipt are both durable.

Derived workers restart only after authoritative readiness is reproved. They
recover from the restored authoritative frontier under ADR-0049. No projection,
outbox accelerator, cursor, session, subscriber, or process-generation state
from before drain is reused.

### Persist a narrow external maintenance receipt ledger

The configured `backup_root` contains one reserved service-owned subtree:

```text
<backup_root>/.maintenance/
```

`BackupNameV1` can never address it. It is disjoint from named immutable backup
artifact directories and from the database data directory. Only the
maintenance adapter may read or write it.

The subtree contains one versioned, checksummed, atomically replaced receipt
per `OfflineMaintenanceOperationId`. A receipt has immutable semantic input
and a bounded monotonic phase history. At minimum it records:

- receipt format version and operation ID;
- operation kind and canonical input hash;
- checked backup name;
- admitted actor/capability identity in the existing redacted audit form;
- applicable approval identity;
- source/staged `DatabaseId`;
- backup manifest identity and included application frontier when known;
- the closed phases `Accepted`, `Draining`, `Offline`, `ArtifactPublished`,
  `Validating`, `Succeeded`, and `FailedClosed`;
- a closed safe failure code; and
- the checksum of the complete canonical receipt body.

One receipt has at most 16 monotonic transitions. Rewriting uses a new sibling
temporary file, complete write, file sync, atomic rename, and parent-directory
sync. A failed parent sync makes the result uncertain. The exact encoding,
field order, checksum preimage, file naming, maximum byte size, and golden
fixtures are frozen by WP-155 before implementation of the driver. SHA-256
remains owned by `riffdb-storage-redb`; no new checksum dependency owner is
added.

On startup, the maintenance controller validates every receipt before ordinary
readiness. An unknown version, bad checksum, noncanonical input, duplicate
operation ID, phase regression, impossible transition, or receipt/artifact
mismatch fails closed. An incomplete receipt is reconciled only from exact
filesystem and checksum evidence:

- a named backup is published only if its complete immutable inventory and
  manifest match the receipt input;
- a restored target is published only if its complete artifact checksum and
  manifest match the receipt; and
- otherwise the operation remains safely failed/uncertain and no success is
  invented.

This subtree is durable operational state but is not RiffDB database metadata,
not part of the `StoredEnvelope` registry, not a redb table/key, not copied into
a backup artifact, and not authoritative application history. It is the sole
POC exception used to audit and resolve an offline maintenance operation.
Changing its encoding or semantics requires compatibility and recovery review.

### Accept the explicit restore-rewind limitation

Restore preserves the exact `DatabaseId` stored in the backup. It does not mint
a new database, node, process, history, or incarnation identifier.

After destructive restore, every observation created after the backup's
included frontier is invalid, including:

- command outcome and idempotency assumptions;
- commit, provenance, event, outbox, projection, and audit locators;
- pagination cursors, subscriptions, sessions, process generations, and
  read-after-sequence expectations;
- capability changes and authorization facts; and
- external facts inferred from the destroyed suffix.

The restored application and administration allocators resume from the backup.
Destroyed application and administration sequence suffixes may therefore be
reused for different future records. An idempotency key whose row existed only
in the destroyed suffix may be accepted again. The database provides no
incarnation component with which a client can distinguish old and new uses of a
reused sequence.

Documentation, CLI confirmation, release notes, and the POC known-limitations
report must state this prominently. A durable incarnation/history-epoch,
globally stable pre-restore locators, tombstone journal, point-in-time recovery,
and non-reusing sequence policy are deferred beyond the POC and require a new
ADR. No implementation may imply that preserved `DatabaseId` preserves the
destroyed history.

### Superseded limitation (ADR-0072)

ADR-0072 adds a durable, monotonic `history_incarnation` retained-metadata
fence bumped only on destructive restore. Sequence-anchored public requests may
optionally validate an `observed_history_incarnation` and receive
`RDB-HISTORY-0101` on mismatch. Sequence reuse after restore remains possible;
the limitation narrows from "undetectable" to "detectable by participating
clients." Residual risk for non-participating clients remains documented.

## Consequences

- Backup and restore use the normal public-client and shared service/policy
  boundaries without giving CLI, gRPC, or MCP a redb path.
- A distinct receipt format is required because the operation intentionally
  closes or replaces the database that owns normal audit records.
- The database's exact six metadata categories and 27-readable/26-writable
  durable registry remain unchanged.
- Restore can recover an empty or corrupt target without a principal-less
  bypass by authenticating against the fully validated staged backup.
- Protected polling is unavailable during the offline interval.
- The POC has a documented destructive-rewind limitation until an incarnation
  design is accepted.

## Rejected Alternatives

1. **Expose WP-070 file copy directly in CLI:** rejected because it bypasses
   API-neutral authorization, lifecycle, audit, and uncertainty recovery.
2. **Run backup while redb remains open:** deferred; online backup is an alpha
   feature and requires a different consistency contract.
3. **Append a normal service-audit terminal after restore:** rejected because
   it mutates the restored snapshot and invents an administration suffix.
4. **Store the receipt in redb:** rejected because it would be unavailable
   during offline work and overwritten by restore.
5. **Authorize an empty/corrupt restore from the current database:** rejected
   because no trustworthy current authorization state exists.
6. **Trust only current authorization for healthy replacement:** rejected
   because the restoring bearer might not exist or be authorized in the backup
   that will become current.
7. **Carry an authenticated principal across restore:** rejected because the
   staged database must make a fresh authentication and authorization decision.
8. **Add a separate recovery credential:** rejected; the ordinary bearer is
   retained only long enough to authenticate against the validated staged
   backup.
9. **Expose maintenance through MCP:** rejected for the POC because destructive
   offline administration is outside the accepted MCP tool/risk model.
10. **Mint a new `DatabaseId` on restore:** rejected because restore preserves
    the backed-up database identity and would break manifest and durable links.
11. **Claim sequences never repeat after rewind:** rejected because no
    incarnation or external allocator exists in the POC.

## Compatibility

The three public RPCs and their messages are additive pre-release public
protocol changes owned by WP-155. Descriptor, source, schema inventory, field
number, structural-validation, wire, SDK, CLI JSONL, and fuzz fixtures must be
accepted and reproducible before implementation merges. The public service
inventory becomes exactly five services and 25 RPCs.

The maintenance receipt is a new versioned external durable format and requires
golden, corruption, upgrade-rejection, and crash fixtures. It does not alter
the redb table layout, `StoredEnvelope`, storage format version, backup manifest
v1, durable Protobuf registry, service-audit operation enum, capability
permission registry, contract IR, MCP registry, or application command API.

## Security

All public paths are loopback POC administration paths. Authorization is
deny-by-default and requires an existing global administrative capability and
approval obligations. A corrupt or absent target grants no authority.
Staged-backup authentication occurs only after complete checksum and semantic
validation. The destructive flag grants no permission. Backup names cannot
escape their configured root. Credentials and internal filesystem paths never
enter receipts, logs, metrics, public errors, or output. Receipt corruption and
ambiguous publication fail closed.

## Testing

WP-155 must include:

- public descriptor/wire/SDK/CLI goldens for the exact three operations,
  checked UUIDv7 operation IDs, backup-name grammar, replacement enum, unknown
  fields, size limits, and stable retry identity;
- architecture tests proving gRPC/SDK/CLI use the API-neutral service and that
  MCP, CLI, and service have no direct redb maintenance handle;
- current plus staged authorization tests for healthy replacement, staged-only
  tests for empty/corrupt recovery, approval and global-scope denial, credential
  zeroization/redaction canaries, and no reused authorization result;
- receipt exact-byte/checksum/canonical-order/version/transition fixtures and
  parser fuzzing;
- process failpoints before/after receipt sync, drain, close, backup publish,
  staged restore, staged validation/auth, target publication, post-publication
  validation, terminal receipt, and readiness;
- nonempty confirmation and path-traversal/symlink/race rejection;
- no-request/no-poll evidence while offline and complete bounded quiescence;
- successful reopen only after full structural/catalog/digest/capability/audit
  validation and derived-worker restart;
- preservation of `DatabaseId`, removal and possible reuse of application and
  administration suffixes, invalidation of cursors/sessions/locators/outcomes,
  and same-key behavior after rewind; and
- repeated same-operation retries that neither publish a second artifact nor
  repeat a destructive restore.

WP-190 repeats the process-level failure matrix and WP-200 verifies the shipped
documentation, systemd lifecycle, release artifacts, and known limitation.

## Requirements and Work Packages

- **Requirements:** `SYS-001`, `API-001`, `ID-001`, `ID-005`, `STO-020`,
  `STO-021`, `STO-022`, `REC-001`, `REC-002`, `SEC-001`, `SEC-002`,
  `SEC-003`, `POC-008`, `POC-009`, and `POC-010`
- **Defines:** `WP-155`
- **Corrective consumers:** `WP-070`, `WP-120`, `WP-127`, `WP-130`,
  `WP-150`, and `WP-185`
- **Final evidence:** `WP-190` and `WP-200`

Any proposal to add online backup, remote/object storage, an MCP maintenance
surface, a direct file path, a principal-less restore, a new durable capability
or service-audit enum value, a second backup format, a different rewind rule,
or a database incarnation requires new human review.
