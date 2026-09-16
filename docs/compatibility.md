# Compatibility

RiffDB 0.1.0 is a pre-release POC. Compatibility is checked at explicit
boundaries; it is not a blanket promise that arbitrary commits can be mixed.

## Boundaries

| Boundary | POC policy |
|---|---|
| Public gRPC | Versioned `riffdb.v1` Protobuf, reserved-field policy, descriptor and wire fixtures |
| Durable records | Versioned Protobuf payloads inside checked stored envelopes |
| Storage | redb baseline with startup format/integrity validation plus an exact alpha release manifest |
| Contract IR | Versioned deterministic IR and plan-hash domains |
| Migration IR | Exact-parent `MigrationBundleV1`, domain-separated source and bundle hashes, canonical binary goldens |
| MCP | Fixed protocol baseline, generated schemas, stable compiler-owned command names |
| CLI machine output | Closed `riffdb.cli.output/v1` JSON envelope |
| Backup | Immutable manifest v1 plus checksums and exact database identity |
| Maintenance receipt | Separate versioned external format beneath `.maintenance` |

Generated source, descriptors, schema inventories, wire vectors, contract
fixtures, and interface checkpoints are compatibility evidence. The current
source release also carries:

RiffQL V9, query IR V12, and query-module V12 are additive identities for the
compiler-declared `Limit<MAX>` page type. V1 through V8 language sources and V1
through V11 IR/module artifacts retain their existing writers and strict
readers. A bounded maximum is immutable operation identity: changing it
requires module/application rotation and invalidates predecessor cursors, but
does not migrate entity data or change storage or public Protobuf formats.

ADR-0159 adds a process-local named-query cursor hash domain that excludes only
compiler-proved ordinary page-cardinality values. Shared parameter hashes,
query/IR/module/plan/lock identities, public opaque token bytes, protocols,
storage, and durable formats are unchanged. Cursor registries already do not
survive process restart, so predecessor tokens are not reinterpreted. A
resumed ordinary ordered query may change its valid submitted page size while
all invariant parameters, authority, snapshot, order, and provider epoch stay
bound.

- `release/version-topology-v1.json`, the cross-domain map of independently
  owned reader/writer windows, writer policy, lifecycle, source assertions, and
  evidence described in [Versioning and Retirement](versioning-and-retirement.md);

- `release/durable-format-manifest-v1.json`, the exact epoch/writer and complete
  readable/writable record, redb-layout, journal, backup, marker, and named
  receipt-family inventory plus the minimum/maximum supported source release;
- `fixtures/compatibility/release-pairs-v1.json`, the closed supported upgrade
  graph;
- `fixtures/compatibility/physical-formats-v1.json`, the redb, journal, backup,
  receipt, history-incarnation, retention, and recovery-validation inventory;
  and
- `fixtures/compatibility/durable-fixture-inventory-v1.txt`, whose SHA-256 is
  bound into the release manifest.

Run `./scripts/check-version-topology` to verify all release-significant version
domains, `./scripts/check-durable-format-manifest` to verify the physical
boundary directly, or `./scripts/check-generated` to verify both with every
generated artifact. A release bundle is rejected if the manifest, upgrade
table, fixture digest, export posture, downgrade posture, or known limitations
are absent or stale.

The current pre-alpha identity is alpha epoch `1`, writer `1`. Downgrade is
unsupported. A format marker from another epoch or writer is not evidence that
its bytes are readable merely because a decoder accepts some records.

Capability records follow a least-successor rule: ordinary grants remain V1,
migration authority uses migration-only V2, installation authority uses V3,
and a compiler-bound row-policy grant uses V4. A grant with explicit symbolic
application-export authority uses V5; V5 is not selected merely because a
grant can read entities, events, provenance, audit, or administer
capabilities. V5 preserves the exact optional V2 through V4 extensions and
adds a bounded, lineage-ordered export extension. Principal-filtered export
requires the matching V4 role/policy extension, while whole-application export
requires explicit global/all-partition authority. The public export operation
uses that authority for its shared-service, gRPC, Rust operator-client, and CLI
safe points; it never derives export access from an older permission.

The V4 extension binds one exact application-role hash, canonical principal
facts, and canonical protected entity/operation selections; omitting the
extension under the V4 identity is corruption. V1 through V4 source,
descriptor, hash, and wire fixtures remain frozen. V2's source and schema hash
are frozen to ADR-0089's original bytes. A short-lived pre-alpha build
accidentally emitted the additive V3 installation payload under V2's compact
identity; current readers recover that exact payload without dropping
authority, while every new installation grant is written as V3. No general
unknown-field or best-effort durable decoder is enabled by this narrow
compatibility repair.

Normal daemon startup performs the source-free format comparison before redb
can open the file. An exact mismatch exits with `RDB-FORMAT-0101`, the retained
and binary epoch/writer identities, the sole manifest-authorized action, backup
and downtime requirements, and a safe next command. It never falls through to
a decoder-shaped error, creates a replacement database, or enters backup
recovery merely because the format is incompatible.

For a new path, RiffDB durably publishes current-format initialization intent
before creating the redb container. A crash with only that marker (or an empty
container) resumes initialization on the next start; it is never mistaken for
a predecessor database that needs an offline upgrade.

The current durable-record registry adds the private clean-close lifecycle
record through one exact predecessor-to-successor registry migration. That
migration does not synthesize clean evidence: the first open performs complete
validation and records dirty lifecycle state. A matching clean lifecycle record
is eligible only under the exact successor registry digest. Older binaries do
not recognize that registry and downgrade remains unsupported.

Clean-close compatibility is exact rather than best effort:

| Offline operation | Certificate bytes | Next startup decision |
|---|---|---|
| Immutable named backup creation | Preserved in the verified artifact | Private restore staging first runs complete exact-end validation; the later staged-authorization open consumes matching CLEAN into DIRTY before publication |
| Destructive restore or maintenance replacement | May be copied as source bytes | Invalidated by the new history incarnation; complete validation |
| Complete stopped lifecycle-unit copy | Preserved with its matching journal and format marker | Preserved while every bound root and identity remains exact |
| Bare database-file copy | May be copied | Ineligible without the matching journal, marker, and lifecycle unit; complete validation |
| Registry migration or incompatible internal format | Old bytes may remain readable during the transition | Invalidated or refused fail-closed; complete validation is never skipped |
| Compatible marker-only format upgrade | Preserved with the unchanged internal lifecycle unit | Preserved after the backup-bound marker transition |
| History-incarnation rotation | Preserved until ordinary lifecycle replacement | Invalidated; complete validation |
| Retention hold change or prune | Preserved until ordinary lifecycle replacement | Invalidated by changed retention roots; complete validation |
| Sealed internal integrity scrub | Engine repair, journal recovery, or compatible registry migration may physically change bytes | Complete validation fails closed as applicable, creates no eligibility, and makes no new semantic application-authority mutation |

Only an unchanged, completely and gracefully stopped lifecycle unit can reuse a
matching record. The sealed primitive and its internal receipt are not public;
the authorized maintenance operation and durable receipt are not yet available.

The current registry also adds durable tag 67
`StoredColumnarProjectionControlV1` and the `columnar_projection_controls`
table. New scalar and vector controls always begin from authoritative state at
a `BeforeFirst` fence; existing scalar manifests, name-derived directories, and
tag-65 vector controls are not migrated. Tag 65 remains structurally readable
under a 4,096-row startup bound but is inert and unwritable until its mandatory
epoch-2 removal. Controlled V1 manifests are immutable and selected only by the
exact length and checksum in tag 67.

The same tag-67 control now supports the bounded per-database transition from a
selected V1 generation to an additive immutable V2 generation. V1 remains
readable and writable until one complete V2 `ROOT-V1` is durably selected; an
incomplete candidate is never a compatibility signal. Segment V2,
encoding-registry V1, Manifest V2, and generation-root V1 retain their exact
registered readers, writers, fixtures, bounds, and definition bytes. Once V2
is selected, open requires the exact control-bound generation, history
incarnation, frontier, root identity, partition inventory, manifests, segments,
and format tuple. Missing, extra, mixed V1/V2, stale-incarnation, unknown, or
corrupt selected material fails closed without V1 fallback. Downgrade remains
unsupported; a future removal of V1 identities still requires its separately
governed retirement package.

WP-749 registers successor command capsule V7 (tag 54, revision 6) and segment
V6 (tag 55, revision 6) for exact command-prefix evidence. Earlier record bytes
remain readable by their own codecs and do not acquire intermediate values.
The registry digest changes: a database or backup carrying the previous exact
registry marker is refused by this binary before opening or replacement. Use
its matching binary; this addition declares no automatic migration edge.
For received successor command groups, the follower checks evidence against the
original receipt and the exact starting rows, including keys that disappear
from the receipt because later commands restore their original values. A
contradiction refuses the frame without advancing durable progress. Newly
committed audited commands now write these successor identities and retain
their intermediate entity/index state atomically. Their complete evidence copy
counts toward the existing pre-sequence capacity reservation, so a command near
the previous encoded limit may now be refused before receiving a sequence.
Startup, recovery and received successor groups also check that retained entity
images, chain heads and index epochs exactly match their owning command facts;
missing, extra or substituted rows refuse even when their envelopes are valid.
Pending-idempotency mutations may only delete that command's identity. Followers
also join entity, chain-head and epoch transitions to their exact logical prior
rows, including earlier commands in the same group. Retained-segment decoding
checks known intra-segment predecessors and their raw mutation preconditions.
A wrong nested record type is corruption, never a legacy-format fallback.
Secondary-index prefix puts also bind their typed key and schema to the command
and require the exact owning partition/index epoch. Available prior index rows
are checked against that same generation bucket before replacement or deletion.
Startup and follower replay also resolve supplied index keys through the retained
command contract and derive each put's key and exact covered fields from its
entity post-image. Undeclared or substituted covered fields refuse even when
the receipt and prefix agree. Supplied puts must also name the catalog-derived
owning partition. Followers derive the complete required index puts and deletes
from each entity's actual predecessor and post-image, refusing missing or extra
mutations. Startup proves the same inventory for creates and known intra-segment
predecessors; it does not invent unavailable historical entity values. Each
available prior index row, including unchanged indexes, must match the key,
covered fields and owning partition derived from its predecessor entity. Missing
or contradictory known rows refuse before durable follower progress. Expected
covers are derived one at a time. Older writer records use the existing bounded
lineage proof to materialize eligible optional fields as null before deriving
index contents, while transition hashes still bind the raw writer bytes.
Every new prefix entity image now requires the complete field set declared by
its exact writer schema, including explicit optional nulls. Catalog checks validate
value types and bounds, primary-key field values, command partition route, and the
retained executable plan. Extra or missing fields refuse even when no index or
vector uses them; historical predecessor materialization remains separate.
Vector prefix rows now require canonical keys and typed values. Written evidence
binds the entity image, version, vector presence, command plan, sequence,
provenance and partition. Every evidence mutation requires its reciprocal index
mutation and partition/lineage observations; missing, extra or contradictory
links refuse. Observation revisions must match the command, and persisted
partition counts cannot be empty. Catalog checks reject undeclared vector fields
and validate rewritten embedding metadata against the production declaration.
Known entity predecessors determine required source/embedding writes and deletes,
including refusal when all vector work is omitted. Available prior evidence and
its reciprocal index must agree; unchanged fields preserve earlier evidence,
and source-only writes preserve embedding stamps exactly. When evidence and
observation predecessors are available, validation folds every affected entity
into its shared partition counter and checks exact total, stale and per-model
counts. It also derives the lineage health summary with the compiler-owned
thresholds. Resealed counter and health forgeries refuse before follower progress.
Unknown retained predecessors remain unproven. Complete cross-history and candidate
graph validation, and exact-stop archive restore remain unfinished WP-749 work.

Inspect the format while the server is stopped:

```bash
riffdb storage preflight --database-path "$HOME/.local/share/riffdb/riffdb.redb"
```

The sole supported alpha-1 predecessor edge is an offline, backup-required,
one-way transition from writer 0 to writer 1:

```bash
riffdb storage upgrade \
  --database-path "$HOME/.local/share/riffdb/riffdb.redb" \
  --backup "$HOME/.local/share/riffdb/backups/pre-format-upgrade"
```

The backup must have been produced and verified by the source release after
writes stopped. The upgrade binds its exact manifest and database identity in a
checksummed sibling receipt, resumes safely across accepted/migrated/marker
crash boundaries, and reports success only after the current marker is durable
and revalidated. There is deliberately no `--force`, `--ignore`, `--reset`, or
downgrade option.

The production storage engine in `riffdbd` is redb. Any Fjall comparison lives
in an isolated nested workspace and cannot be substituted into the server.

## Upgrade Rules

- Start a database only with a binary that recognizes its storage and durable
  format versions.
- Never use a force, ignore, reset, or best-effort decoder option to cross a
  durable-format mismatch. RiffDB defines no such compatible action.
- Never edit stored envelopes, maintenance receipts, manifests, generated
  descriptors, or contract IR by hand.
- Do not deploy two independently changed public/durable schemas under the same
  version.
- A downgrade is unsupported.
- Copying a database file does not grant clean-start eligibility in another
  configured database. Clean evidence remains bound to the stored database
  identity, history incarnation, registry, frontiers, journal, and bounded
  roots; any mismatch selects complete validation.
- A backup should be restored with the release family that created and verifies
  its declared restore range before any later migration is attempted. A legacy
  backup without a physical compatibility range is inspected but not restored
  by the current binary; restore it with its source release, validate it, take
  the prescribed pre-upgrade backup, then run the exact offline transition.
- Digest-key rotation is compatible only while every key needed to read
  retained records remains present with its original ID and material.
- A migration targets one exact parent bundle and one exact canonical
  successor. It is never inferred from version numbers or chained through an
  intermediate contract.
- Application Source V3 and Lock V4 are additive formats. V1/V2 source and
  V1/V2/V3 lock decoders remain strict compatibility boundaries.

Restore preserves the backed-up `DatabaseId` but does not preserve observations
from the destroyed suffix. It is therefore not a mechanism for maintaining
globally stable post-backup locators. ADR-0072's durable `history_incarnation`
makes restore rewinds detectable for clients that send optional
`observed_history_incarnation`; non-participating clients remain unvalidated.
