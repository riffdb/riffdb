# ADR-0042: Sealed Catalog-Owned Index Migration Driver

- **Status:** Accepted
- **Direction approved:** 2026-07-22
- **Exact text accepted:** 2026-07-22
- **Accepted:** 2026-07-22
- **Acceptance reference:** Human maintainer confirmation in the current Codex
  session on 2026-07-22
- **Requires:** ADR-0004, ADR-0006, ADR-0016, ADR-0030, ADR-0038, and
  ADR-0039
- **Amends:** ADR-0039's migration type ownership and corrective sequencing;
  SPEC Sections 5.2, 5.3, 10.1, 10.6, 19.4, and the ADR register; and the
  affected WP-050 through WP-130 work-package dependencies, allowed paths, and
  deliverables
- **Decision deadline:** Before the ADR-0039 migration implementation is
  claimed complete, before WP-130 completes, and before the P1 gate

The human maintainer explicitly confirmed acceptance of this decision in the
current Codex session on 2026-07-22. This record makes only the authority and
dependency correction described below. It changes no durable bytes, Protobuf
message, public protocol, storage key, page bound, transaction rule, or
migration result.

## Context

ADR-0039 requires catalog to be the sole authority that derives a historical
partition and turns a codec-checked migration row into one `V1Rewrite` or
`V2Confirm` instruction. It also requires the concrete backend to be the sole
authority that proves row provenance, reads the current bytes, and atomically
compares and applies a complete batch.

The first cross-crate type split did not enforce that ownership. Public
storage-API authority/tracker/advance/completion values and a public migration
page/instruction chain allowed a caller that possessed a historical exact-end
value to reproduce catalog progress without consuming
`CatalogIndexMigrationContext`. Documentation, `#[doc(hidden)]`, source scans,
and private fields on only part of that chain cannot seal a public constructor
graph. The same split placed catalog-semantic instructions in
`riffdb-storage-api`, even though storage API is intentionally IR-blind and
must not own historical interpretation.

A sound boundary must make the invalid composition unrepresentable across
crates. Catalog must own every value that claims historical derivation or
catalog completion. Concrete backends must keep the page, point-read, apply,
and exact-end transitions that claim physical provenance private. The server
must be able only to join the two matching migration-required outcomes and ask
the shared catalog driver to run them.

## Decision

### Keep storage API semantic and identity-only

`riffdb-storage-api` retains only the migration values that belong to the
semantic storage contract:

- the codec-owned `IndexMigrationSemanticRow` closed V1/V2 value;
- the fields-private, process-local `IndexMigrationRowEvidence` association
  whose production constructor remains owned by the durable codec;
- the checked migration cursor, identity, charge, count, and byte-bound values;
- `StructuralOpenOutcome`; and
- an identity-only `StartupIndexMigrationPort` trait/type contract bound to the
  exact `DatabaseId` and `OpenSessionId` of the structural session that
  produced its concrete port.

The identity-only contract is not an operational storage trait. It exposes no
row scan, bundle read, instruction construction, batch apply, exact-end,
finish, or dormant-port transition. External test or comparison backends may
implement that identity-only contract; such an implementation grants no
migration operation or catalog proof. Each production concrete port has a
private constructor and can be produced only by that backend's consumed
structural session.

The following authority-bearing surface is removed from
`riffdb-storage-api`:

- `IndexMigrationCatalogAuthority`, `IndexMigrationCatalogTracker`,
  `IndexMigrationCatalogAdvance`, and `IndexMigrationCatalogCompletion`;
- the storage-API-owned migration instruction, page, row-work, pending-batch,
  and instruction-batch chain;
- `StartupIndexMigrationEnd` and `StartupIndexMigrationRead`; and
- every public or publicly implementable migration page-read, point-read,
  apply, or finish operation.

Storage API therefore cannot mint catalog progress or completion, and
implementing the identity-only contract cannot obtain or replay backend
transition values. This amendment does not change the codec factory's
existing responsibility for proving the exact physical-key, canonical-envelope,
and semantic-row association or for constructing a canonical V2 replacement
envelope.

### Make catalog the sole migration-semantic owner

`riffdb-catalog` owns the fields-private, move-only migration instruction,
instruction batch, pending-batch state, completion, and consuming driver. None
is serializable, cloneable, durably persisted, or constructible from public
parts. A completion can be produced only when the catalog-owned driver has
consumed its original `CatalogIndexMigrationContext`, every row in every page,
and the backend-private exact end for the same database and open session.

The sole row-derivation operation accepts the current codec-checked row and the
exact owned, canonically checked historical bundle selected for that row. It
does not accept a caller-supplied partition, V2 post-image, instruction,
completion counter, or arbitrary bundle reference. Catalog selects the
historical owner and schema, decodes the complete embedded entity key, derives
the positional root prefix, evaluates the accepted historical partition
expression through the existing pure `riffdb-invariant` edge, and then creates
exactly one closed instruction:

- `V1Rewrite`, binding the complete expected V1 evidence and catalog-derived
  semantic V2 post-image; or
- `V2Confirm`, binding the complete expected V2 evidence only after the stored
  partition equals the catalog-derived partition.

The catalog driver consumes one matching
`CatalogIndexMigrationContext` and one concrete backend migration port. Its
cross-crate backend calls are guarded by catalog-owned, fields-private,
move-only request/proof values branded by the exact concrete backend type `B`.
Every scan, one-bundle read, batch apply, and finish call consumes one such
branded request/proof. The corresponding backend response factory may be
invoked only by consuming that exact request/proof and returns the next branded
state; there is no constructor from a database/session identity or unbranded
parts. This means a local `Fake` backend cannot capture a catalog-owned
`Batch<Fake>` and replay, convert, or present it as `Batch<Redb>`. A named public
migration-only backend trait is permitted only under this branding and
consuming-factory rule. The catalog-owned branded request and response types are
public only as required for a concrete crate to implement that trait; all
fields and production factories are private or require consumption of the
prior branded state. Server code is forbidden from importing or receiving any
of them. Any minimal backend-facing view of an instruction is read-only and
available only while the driver is consuming that instruction; it exposes no
constructor, IR, bundle, policy value, callback, or reusable authority.

### Give each concrete backend one narrow catalog edge

`riffdb-storage-memory` and `riffdb-storage-redb` may each depend directly on
`riffdb-catalog` only for the catalog-owned startup index-migration driver and
its opaque linear values. This is a narrow concrete-adapter dependency, not a
general storage dependency rule. It grants neither backend permission to
import `riffdb-contract-ir` or `riffdb-invariant`, interpret a historical
partition expression, validate catalog history, create
`ValidatedCatalogHistory`, compose readiness, or access catalog persistence.

Each backend implements one named concrete port:

- `MemoryStartupIndexMigrationPort`; or
- `RedbStartupIndexMigrationPort`.

The concrete port privately implements the storage-API identity-only contract
and is produced by its structural session. All concrete backend row-page,
one-bundle-read, pending-apply, batch-applied, exact-end, and finish helper
states are module-private. Catalog-owned branded request/response types may be
public only for the backend trait implementation; their fields and factories
remain sealed as above. They are not re-exported by a concrete backend or
visible to the server. The only server-usable operation is the consuming
catalog-owned drive entry that takes the matching catalog context and concrete
port and returns the backend's dormant unopened state or a typed failure.

`riffdb-storage-memory` and `riffdb-storage-redb` may use
`riffdb-contract-compiler` as a default-feature-disabled dev-dependency only in
conformance/recovery fixtures that build a real canonical indexed contract
bundle and exercise complete catalog validation. This grants no production
compiler or IR dependency, no expression interpretation in either backend, and
no fixture-only shortcut in production startup.

The accepted ADR-0039 mechanics remain unchanged inside that sealed
composition:

1. The backend scans strict physical-key order through short read transactions
   and constructs session-bound codec evidence.
2. For the current row only, the backend obtains the exact retained bundle in
   a short read transaction, closes the transaction, and gives the owned
   canonical bundle to catalog derivation. No storage transaction is held while
   catalog decodes a key, evaluates an expression, or retains an instruction.
3. Catalog consumes each row into exactly one instruction and closes only a
   complete page into one instruction batch.
4. The backend consumes that opaque batch in one short write transaction,
   rechecks every expected current byte, constructs replacements through the
   durable codec, and commits all replacements or none.
5. Only a backend-private exact end joined to the catalog driver's exact
   same-session completion can produce dormant unopened backend state.

The existing independent 500-row/4-MiB evidence and conservative instruction
ledgers, strictly advancing continuation, one-maximum-row proof, idempotent V2
replay, `V2Confirm` no-write behavior, cancellation release, failpoints, and
fresh post-migration validation are not weakened.

### Restrict server composition

`riffdb-server` may inspect only the closed startup outcomes. It joins
`Ready` with `Clean`, rejects crossed outcomes, or consumes matching
`MigrationRequired` values by invoking the catalog-owned driver with the
catalog context and named redb port. Server code cannot name, inspect,
construct, retain, reorder, or apply a migration row, instruction, page, batch,
point-read state, exact-end state, or completion. After a successful drive it
receives only dormant unopened redb state, discards all prior evidence, and
starts the already required fresh validation session.

No gRPC, MCP, CLI, SDK, service, runtime, commit, policy, or operational storage
path receives a migration constructor or backend transition.

### Correct the package dependency and merge order

The crate dependency direction is exactly:

```text
riffdb-catalog -> riffdb-storage-api
riffdb-storage-memory -> riffdb-storage-api, riffdb-catalog
riffdb-storage-redb -> riffdb-storage-api, riffdb-catalog, redb
riffdb-server -> riffdb-catalog, riffdb-storage-redb
```

There is no catalog-to-concrete-backend edge and therefore no crate cycle. The
existing catalog-to-invariant edge remains the sole historical partition
evaluator edge; neither storage backend gains an IR or invariant dependency.

The work-package order is corrected as follows:

1. WP-060 owns the storage-API semantic row/evidence, bounds, identity-only
   startup outcome, and ordinary memory structural session. It does not own a
   complete catalog-driven memory migration implementation.
2. WP-065 continues to own the codec factories and durable compatibility
   evidence. It owns no catalog instruction.
3. WP-050, already dependent on WP-060, owns the opaque instruction states,
   historical derivation, completion, and consuming driver contract.
4. WP-070 gains WP-050 as a hard dependency. After WP-050, WP-060, and WP-065,
   it integrates the named memory and redb ports with the catalog driver and
   supplies their shared conformance and redb crash/restart evidence.
5. WP-130 only composes the accepted driver and owns the process proof. It
   gains no migration-semantic or backend-transition type.

No declared dependency is removed. WP-075, WP-100, WP-120, WP-125, WP-190, and
WP-200 remain corrective consumers or evidence owners and must compile and test
against the sealed interface. ADR-0042 is added to the `required_adrs` of the
same exact affected set as ADR-0039: WP-050, WP-060, WP-065, WP-070, WP-075,
WP-100, WP-120, WP-125, WP-130, WP-190, and WP-200.

## Consequences

- Catalog's semantic authority is enforced by construction rather than by
  documentation or source scanning.
- Concrete storage crates gain one upward dependency that is deliberately
  confined to offline startup migration. This is preferable to moving
  catalog-semantic instructions into an IR-blind foundation crate.
- Memory conformance and redb recovery tests gain one default-feature-disabled
  compiler dev-dependency solely to construct canonical indexed catalog
  fixtures; the production graph gains no compiler or IR edge.
- WP-070 cannot finish before WP-050, so the hard DAG now records the order the
  type boundary actually requires.
- Backend conformance tests may require backend-owned test helpers, but those
  helpers remain under `cfg(test)` and cannot create production catalog proofs
  or public migration operations.
- No stored database, compatibility fixture, protocol client, or application
  service observes a format or behavior change.

## Rejected Alternatives

1. **Keep public storage-API trackers and rely on private fields or
   `#[doc(hidden)]`:** rejected because public methods still compose a second
   catalog-completion authority.
2. **Seal with source-text architecture tests only:** rejected because those
   tests detect known spellings but do not make invalid construction
   impossible.
3. **Move historical derivation into storage:** rejected because it creates a
   storage-to-IR dependency and a second expression interpreter.
4. **Make catalog depend on concrete backends:** rejected because it reverses
   and couples the semantic layer to concrete engines and conflicts with the
   chosen concrete-backend-to-catalog adapter edge.
5. **Expose a generic callback or public page/apply trait to server:** rejected
   because it exposes the transition chain the seal is intended to remove.
6. **Persist a migration marker or continuation:** rejected by ADR-0039; fresh
   complete validation remains the only completion proof.

## Verification

- Storage-API architecture tests prove the removed authority/tracker/advance/
  completion, instruction/page/batch, `StartupIndexMigrationEnd`,
  `StartupIndexMigrationRead`, and public page-read surfaces do not exist.
- Catalog compile-fail and type-state tests prove external code cannot
  construct an instruction, pending state, batch, completion, or derived
  partition result, and cannot complete without consuming every row and exact
  end from one session.
- Memory and redb architecture tests prove only their startup migration modules
  import catalog, neither imports contract IR or invariant, backend
  helper/intermediate states are private, catalog-owned branded types expose no
  fields or free factories, and no operational trait exposes migration.
- Server architecture tests prove it names only the catalog context, concrete
  redb port, catalog drive entry, and dormant result; it cannot name any
  instruction or backend intermediate.
- Memory/redb conformance retains all ADR-0039 ordering, 500/501-row,
  4-MiB/equal-plus-one, point-read, dual-ledger, whole-batch, idempotence,
  cancellation, and crossed-session cases.
- Redb transaction instrumentation proves every row and bundle read
  transaction is closed before catalog evaluation and every batch write
  transaction is bounded to compare-and-rewrite.
- Process failpoints and WP-130 restart evidence prove only dormant unopened
  state follows migration and only a fresh V2-only `Ready` plus `Clean` session
  reaches readiness.

## Requirements and Work Packages

- **Requirements:** `SYS-004`, `STO-001`, `STO-002`, `STO-012`, `STO-020`,
  `STO-021`, `STO-022`, `REC-001`, `REC-002`, `TXN-041`, `TXN-043`,
  `API-001`, `VAL-003`, `POC-008`, and `POC-009`
- **Interfaces corrected or blocked:** `WP-050`, `WP-060`, `WP-065`,
  `WP-070`, `WP-075`, `WP-100`, `WP-120`, `WP-125`, and `WP-130`
- **Final recovery and exit evidence:** `WP-190` and `WP-200`

Implementation may proceed only with this sealed ownership. Any proposal to
restore a public migration transition, expose backend intermediate state, let
the server inspect instructions, accept a caller-derived partition, add a
storage-to-IR edge, or change durable/protocol bytes requires new human review.
