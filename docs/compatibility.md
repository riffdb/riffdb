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

Run `./scripts/check-durable-format-manifest` to verify this boundary directly,
or `./scripts/check-generated` to verify it with every generated artifact. A
release bundle is rejected if the manifest, upgrade table, fixture digest,
export posture, downgrade posture, or known limitations are absent or stale.

The current pre-alpha identity is alpha epoch `1`, writer `1`. Downgrade is
unsupported. A format marker from another epoch or writer is not evidence that
its bytes are readable merely because a decoder accepts some records.

Capability records follow a least-successor rule: ordinary grants remain V1,
migration authority uses migration-only V2, and any installation authority uses
the distinct V3 record. V2's source and schema hash are frozen to ADR-0089's
original bytes. A short-lived pre-alpha build accidentally emitted the additive
V3 installation payload under V2's compact identity; current readers recover
that exact payload without dropping authority, while every new installation
grant is written as V3. No general unknown-field or best-effort durable decoder
is enabled by this narrow compatibility repair.

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

Inspect the same decision while the server is stopped:

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
