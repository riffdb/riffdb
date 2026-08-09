# ADR-0112: Alpha Format Compatibility and Application Portability

- **Status:** Proposed
- **Direction proposed:** 2026-08-09
- **Exact text accepted:** No
- **Decision deadline:** Before the first alpha release artifact or WP-575 adds
  an application export operation
- **Requires:** ADR-0007, ADR-0019, ADR-0050, ADR-0055, ADR-0057,
  ADR-0061, ADR-0072, ADR-0075, ADR-0076, ADR-0079, ADR-0080,
  ADR-0082, ADR-0085, ADR-0104, ADR-0110, and ADR-0111
- **Amends if accepted:** ADR-0082's pre-alpha format-acceptance posture
- **Defines or blocks:** WP-574, WP-575, WP-576, WP-578, and WP-579

## Context

RiffDB has versioned durable envelopes, registries, startup validation,
application migration, and verified offline backup/restore. It does not yet
state what one alpha binary promises when opening data written by another.
Without that policy, a format break becomes an empirical discovery.

Application migration is not a database-format upgrade. A backup is a
same-format disaster artifact, not a universal migration format. Alpha also
needs an application-level way to leave: portable symbolic entity/event data
that does not expose storage keys or require the old database forever.

The honest pre-1.0 contract may permit declared breaking format epochs, but it
must make them planned, detectable, receipted, and recoverable through export
and application-owned reimport—not silent reset or raw storage copying.

## Proposed Decision

### Every release publishes one exact durable-format manifest

Each release artifact carries a generated `DurableFormatManifest` that names:

- its alpha format epoch and writer version;
- every readable and writable durable record/registry/storage/journal/backup/
  maintenance-receipt version;
- minimum/maximum supported source release and one-way upgrade edges;
- whether an upgrade is online, offline-in-place, export/reimport-only, or
  unsupported;
- required free space, backup, validation, and downgrade posture; and
- the digest of the compatibility fixtures proving those claims.

Startup reads enough immutable format identity to compare manifests before any
mutation, migration, allocator repair, journal replay, or readiness. Unsupported
or ambiguous combinations fail closed with a typed error naming current epoch,
binary epoch, supported action, and safe command—never an internal decoder
error and never automatic empty-database creation.

Within one declared alpha format epoch, a patch/minor release must either open
and preserve the database byte/semantics according to its manifest or refuse
before mutation. A destructive reset is never an in-epoch upgrade.

### Breaking alpha epochs are permitted only with ceremony

Before 1.0, a release may declare a new incompatible alpha format epoch only if
the preceding supported release can:

1. validate and back up the old database;
2. produce a complete application export and receipt;
3. install an empty new-epoch database and exact application artifacts;
4. reimport through declared compiled commands or an accepted application
   migration—never raw entity/storage writes;
5. reconcile exported/imported identities, counts, hashes, events, and
   adapter-owned observations; and
6. retain old backup/export artifacts until the new database passes full
   startup and conformance validation.

The release notes and CLI preflight name expected downtime and unsupported
data classes. There is no downgrade and no claim that an old physical backup
restores into a new incompatible epoch. Operators who choose not to cross the
epoch can continue running the preceding supported binary against its data.

### Application export is symbolic, snapshot-consistent, and resumable

The public operator surface adds a versioned application export operation
through the shared service, gRPC, Rust operator client, and CLI. The server—not
the CLI—selects one database and one immutable published snapshot/frontier.
The operation emits bounded canonical pages/files for:

- current entities by contract lineage/version and symbolic entity/key/field;
- retained typed domain events with event identity, version, partition,
  sequence, causation/correlation, and payload;
- optional provenance and public-safe audit records under separate explicit
  operator authority; and
- one manifest/receipt binding database/history identity, contract/module
  identities, selected row-policy scope, frontier, counts, byte hashes,
  omissions, and terminal state.

Output contains natural typed application values and stable symbolic names. It
contains no numeric entity/field/index IDs, storage keys/envelopes, capability
tokens, digest keys, internal journal/changelog bytes, hidden schema, or
unredacted submitted secrets. Canonical JSONL is the required alpha carriage;
other formats are derived later.

Pages are bounded by rows/bytes/time and use an opaque export cursor bound to
the operation, snapshot, principal, scope, and format. Export state has a
bounded lease and durable checkpoint. Expiry returns a typed restart outcome;
it never silently splices a newer snapshot. Cancellation and crash leave a
receipted incomplete export, not a falsely complete directory.

Whole-application export requires a distinct operator permission. A
principal-scoped export applies ADR-0111 row/field policy before serialization.
The receipt states which scope was selected. Audit/provenance inclusion never
follows merely from entity visibility.

### Reimport remains compiler-owned

Export is not a generic import/write bypass. An application's conformance
manifest maps each portable record class to declared idempotent import commands
or an accepted migration plan, with explicit handling for historical events
that cannot be regenerated. Every imported entity remains subject to current
contract types, invariants, references, row policies, provenance, outcomes,
and command idempotency. Unsupported historical/audit rehydration is declared
as an omission; it is not synthesized.

The adapter acceptance corpus must prove export-to-empty-database reimport for
the data classes it claims portable and compare application-level observations.
Physical identities and commit sequences may change; the receipt records the
mapping boundary instead of claiming byte-identical history.

### Backup/restore and export serve different failures

ADR-0050 backup/restore remains the fast, exact, offline disaster path within
the compatible format range recorded by the manifest. It preserves physical
database identity and complete supported state, subject to history-incarnation
rewind semantics. Export/reimport is the slower application-level portability
path across an incompatible alpha format or away from RiffDB.

The CLI must explain which path is valid. Restore refuses an incompatible
manifest before replacing the target. Export refuses corrupt/unvalidated state.
Neither path may be represented as successful until its receipt and final
validation are durable.

## Options Considered

1. **Promise no compatibility before 1.0:** rejected because silent reset is
   operationally indistinguishable from data loss.
2. **Promise all alpha durable formats forever:** rejected because it freezes a
   rapidly evolving POC before evidence justifies the cost.
3. **Use physical backup as the portable format:** rejected because it couples
   users to storage/journal versions and cannot honestly cross a format epoch.
4. **Declared format epochs plus symbolic export/reimport:** proposed because it
   is strict about what survives while preserving a safe way out.

## Consequences

- Alpha releases carry a precise compatibility table and typed preflight.
- Breaking epochs remain possible, but require export/reimport evidence and may
  impose declared downtime.
- Applications must declare import semantics for any data they promise to move
  into a new database.
- Full physical history preservation across incompatible epochs, online format
  conversion, downgrade, raw CDC export, and arbitrary storage import remain
  unavailable.

## Security

Export is an operator-authorized bounded read, audited/receipted separately from
application queries. Row/field policy is applied unless explicit whole-
application authority is present. Paths remain server-owned and configured;
clients name an export, never an arbitrary filesystem location. Diagnostics,
manifests, and receipts contain hashes and symbolic identities, not credentials,
raw values, private paths, or hidden-schema existence.

## Standing Design Tests

- **Interface safety:** no binary silently opens an unsupported format, and no
  export/import request expresses a raw table, field ID, storage key, envelope,
  transaction, or policy bypass. The only reimport path is compiled application
  behavior.
- **Scale:** export is snapshot-bound, paged, resumable, and bounded in retained
  state/rows/bytes/time. Format preflight reads bounded registries/manifests;
  neither compatibility nor portability requires whole-state memory.

## Testing

- Manifest old/current/unknown/corrupt fixtures and a release-pair upgrade
  matrix with refusal-before-mutation neuters.
- Process-kill export schedules, lease expiry, resume, checksum, truncation,
  row-policy, redaction, and multi-database tests.
- Entity/event/provenance/audit canonical JSONL goldens and cross-language
  value decoding.
- Export/reimport for all four adapter shapes, including declared omissions and
  application-level reconciliation.
- Compatible physical backup/restore and incompatible-epoch refusal tests.

## Requirements and Work Packages

- **Provisional requirements:** `AFC-001` through `AFC-012` and `EXP-001`
  through `EXP-014`, added to `SPEC.md` only after exact acceptance.
- **Defines or blocks:** WP-574, WP-575, WP-576, WP-578, and WP-579.
- **Final evidence:** WP-578 and WP-579.

## Decision Deadline

Exact acceptance is required before the alpha compatibility promise, format
manifest, export protocol/receipt, or export/reimport release claim is frozen.
