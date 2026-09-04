---
adr: 0180
title: Crate Dependency Direction Repair
status: accepted
tier: guarantee
date: "2026-09-01"
accepted: "2026-09-01"
requires: [ADR-0004, ADR-0035, ADR-0037, ADR-0038, ADR-0042, ADR-0049, ADR-0053,
  ADR-0111, ADR-0113, ADR-0164, ADR-0175]
amends:
  - ADR-0037's client-only riffdb-api-grpc edge
  - ADR-0004's reference in-memory backend
  - SPEC Sections 5.1, 5.2, and 5.3 by adding DEP-001 through DEP-006 and the
    layered allow-list they name
requirements: [DEP-001, DEP-002, DEP-003, DEP-004, DEP-005, DEP-006]
packages: [WP-754, WP-755, WP-756]
obligations:
  - id: OBL-0180-1
    package: WP-754
    proof: storage_backends_depend_only_on_storage_api_types_proto_and_catalog_driver
    says: Neither concrete storage backend carries a production edge to
      riffdb-query-executor, riffdb-query-ir, riffdb-policy, or riffdb-projection.
  - id: OBL-0180-2
    package: WP-754
    proof: executor_owned_snapshot_pages_match_pre_inversion_fixtures
    says: The executor produces byte-identical pages, cursors, counts, and typed
      refusals over the storage-api readers that it produced when the same
      programs ran inside the redb transaction.
  - id: OBL-0180-3
    package: WP-754
    proof: redb_startup_produces_evidence_only_and_drives_no_migration
    says: redb startup produces structural and historical evidence only and issues
      no index-migration instruction.
  - id: OBL-0180-4
    package: WP-755
    proof: client_depends_only_on_proto_tonic_types_errors_and_config
    says: The Rust client manifest names exactly the allow-list in section 3 and
      nothing else from the workspace.
  - id: OBL-0180-5
    package: WP-755
    proof: observability_is_a_leaf_crate
    says: riffdb-observability has no production edge to any crate above
      riffdb-errors.
  - id: OBL-0180-6
    package: WP-756
    proof: check-crate-graph
    says: The workspace crate graph satisfies the layered allow-list on every
      compile target, driven from cargo metadata.
review_triggers:
  - A page, cursor, count, aggregate, or typed refusal would differ from its
    recorded fixture.
  - A policy context, executor type, or compiler plan would cross a storage-api
    trait.
  - A storage encoding, key, journal frame, or durable record would change.
  - A layer exception would be added to the crate-graph allow-list without an ADR
    number.
---
# ADR-0180: Crate Dependency Direction Repair

## Context

SPEC Section 5.2 already states the intended edges. The concrete storage rows allow only storage API, `redb`, and the ADR-0042 migration-only catalog edge; the Rust client allows protocol/Tonic plus ADR-0037 foundational types; and observability is "cross-cutting interfaces without business semantics." No check enforces that table, and four areas have drifted.

First, both storage backends depend on `riffdb-query-executor`, `riffdb-query-ir`, `riffdb-policy`, and `riffdb-projection`. They implement `QueryExecutionPort` and composite policy/provider execution inside storage, so a replacement backend must reimplement query semantics. The 21,564-line memory backend mirrors redb.

Second, the 13,453-line `storage-redb/src/startup.rs` drives catalog index migration as well as producing structural and historical evidence, despite ADR-0042 placing the driver in catalog.

Third, `riffdb-client-rust` reaches generated clients and `DATABASE_METADATA_KEY` through `riffdb-api-grpc`, and reaches the contract compiler through `riffdb-application`; ADR-0037 allowed only the reviewed client-side protocol role. Fourth, `riffdb-observability` depends on service, commit, MCP, catalog, policy, conflict, and auth to consume their telemetry types, placing telemetry at the top of the graph. `riffdb-testkit` depends on server and CLI while serving as a dev-dependency of leaf crates, so leaf tests link the daemon across 238 integration targets.


The memory backend is used only by the test kit and four migration gates. It cannot run `riffdbd` because it lacks changelog, backup, restore, and clean-close ports; its remaining parity-oracle role is better served by the model and a reader fake after execution leaves storage.

## Decision

### 1. Concrete backends implement storage-api only

`riffdb-storage-redb` depends in production on `riffdb-storage-api`, `riffdb-types`, `riffdb-proto`, `redb`, and the ADR-0042 catalog driver edge. It has no production edge to query executor, query IR, policy, or projection. `QueryExecutionPort` and `RedbQueryView` leave storage.

### 2. The executor owns composite-query execution over narrow readers

`riffdb-query-executor` implements `QueryExecutionPort` once over the existing point, scan, filtered-scan, snapshot, and ADR-0035 fence readers plus an owned snapshot that pins application head, index epochs, and provider generations for one page and continuation. Policy contexts never cross storage: the executor admits rows before releasing any row, count, cursor, or measure. Storage sees only keys, bounded ranges, and bounded storage-API predicates. ADR-0164 fencing, ADR-0175 partition merge, and all bounds retain their meaning.

### 3. Startup evidence and migration driving separate

`storage-redb/src/startup.rs` retains exclusive read-only structural and historical evidence over one immutable snapshot. Index-migration page, apply, and finish driving moves behind ADR-0042's catalog-owned driver over backend-private branded ports; redb exposes no migration progress.

### 4. The Rust client depends on the protocol only

`riffdb-client-rust` depends on `riffdb-proto`, the Tonic client, and the
ADR-0037 foundational `riffdb-types`, `riffdb-errors`, and `riffdb-config`
edges. Generated Tonic service clients and `DATABASE_METADATA_KEY` move to
`riffdb-proto` behind a client-only feature; `riffdb-api-grpc` keeps the
server side and consumes the same generated messages. The
`ApplicationPortabilityManifest` value the client re-exports moves to
`riffdb-types`, removing the `riffdb-application` edge and, through it, the
contract compiler. This narrows ADR-0037; it grants nothing new.

### 5. Observability is a leaf

`riffdb-observability` depends on `riffdb-types`, `riffdb-errors`, and
tracing. It defines the telemetry event types, observer traits, health
components, and metric names. `riffdb-service`, `riffdb-commit`,
`riffdb-catalog`, `riffdb-policy`, `riffdb-conflict`, `riffdb-auth`, and
`riffdb-api-mcp` implement or emit those types. `riffdb-server` composes them
as today.

### 6. The test kit splits and the in-memory backend is removed

`riffdb-testkit` keeps the reference model, histories, failpoints, scratch,
and inspection helpers with no edge to `riffdb-service`, `riffdb-server`, or
`riffdb-cli`. A new `riffdb-testkit-server` owns temporary servers, process
harnesses, and the root `[[test]]` targets that need the daemon. Leaf crates
take only the core kit as a dev-dependency.

`riffdb-storage-memory` is deleted. After sections 1 and 2 the executor is
tested against a small storage-api reader fake, the existing
`riffdb-testkit::model` authoritative model remains the independent oracle,
and the migration gate tests run against the redb stage. `riffdb-storage-api`
is then trimmed to the traits, records, and bounds that redb, the reader
fake, and the ADR-0113 simulator consume; unused typestate traits and their
fixtures are removed under the ADR-0124 classification rules.

### 7. A cargo-metadata crate graph check enforces layers

`scripts/check-crate-graph`, invoked by `scripts/check-workspace-policy`,
resolves the workspace with `cargo metadata` and rejects any production or
build edge that points upward or sideways across this allow-list:

1. `riffdb-types`, `riffdb-errors`, `riffdb-config`, `riffdb-observability`
2. `riffdb-contract-syntax`, `riffdb-riffql-syntax`, `riffdb-query-syntax`,
   `riffdb-contract-ir`, `riffdb-query-ir`, `riffdb-contract-compiler`,
   `riffdb-query-compiler`, `riffdb-invariant`, `riffdb-proto`
3. `riffdb-storage-api`
4. `riffdb-storage-redb`, `riffdb-catalog`, `riffdb-auth`, `riffdb-policy`,
   `riffdb-projection`, `riffdb-columnar`
5. `riffdb-query-executor`, `riffdb-runtime`, `riffdb-conflict`,
   `riffdb-idempotency`, `riffdb-outbox`, `riffdb-scheduler`,
   `riffdb-query-module`, `riffdb-application`, `riffdb-diagnostics`
6. `riffdb-commit`
7. `riffdb-service`
8. `riffdb-api-grpc`, `riffdb-api-mcp`
9. `riffdb-server`, `riffdb-cli`, `riffdb-mcp-stdio`

`riffdb-client-rust`, `riffdb-driver-host`, and
`riffdb-client-python-native` sit beside layer 2 and may reach only
`riffdb-proto` and layer 1. `riffdb-sim`, both test kits, and
`riffdb-bench-root` are dev-only per `SIM-007`. Named exceptions (the
ADR-0042 catalog edge, ADR-0049's projection edges) are listed in the script
with their ADR number; an exception without an ADR fails the check.

### 8. Build-time evidence is recorded

WP-756 records warm incremental rebuild time after a one-line change in
`riffdb-storage-api`, and per-crate `cargo test --no-run` link time for
`riffdb-policy`, `riffdb-commit`, and `riffdb-service`, before WP-754 and
after WP-756, on the same host, as a non-evidentiary receipt in
`docs/performance/`.

## Options Considered

1. **Leave the edges and document them:** rejected. SPEC 5.2 already documents
   the intended edges; documentation without a check is how drift happened.
2. **Promote the in-memory backend to a server-capable backend:** rejected.
   It would need the changelog, journal, backup, restore, and clean-close
   ports, roughly doubling a 21K-line crate to serve a backend no deployment
   uses. Its parity-oracle role is better served by the model and a reader
   fake once execution leaves storage.
3. **Keep execution in storage and give the executor a storage-neutral
   facade:** rejected. The facade would still require every backend to
   implement policy admission and provider merge, which is the coupling
   under repair.
4. **Move the generated Tonic clients into a new `riffdb-grpc-client` crate
   instead of `riffdb-proto`:** viable, and chosen against only because
   `riffdb-proto` already owns the messages and descriptors; a separate crate
   adds a workspace member for one feature flag's worth of code.

## Consequences

- Storage becomes replaceable in fact: a backend implements readers, the
  command typestate, and durability ports, and inherits query execution.
- Leaf-crate tests stop linking the daemon; incremental rebuilds after a
  storage-api change stop recompiling the executor's callers twice.
- `riffdb-storage-redb` and `riffdb-storage-api` shrink; `startup.rs` loses
  its migration loop; the workspace loses one 21K-line crate.
- Cost: WP-754 touches the hottest read path in the repository and needs
  byte-exact page, cursor, and refusal fixtures before and after.
- The storage-owned V1 query timing census stops emitting samples; any replacement is executor-owned and newly versioned.
- Cost: one new workspace crate (`riffdb-testkit-server`) and one new check.
- Deferred: layer changes for `riffdb-columnar` and the provider adapters in
  `riffdb-server`; a second real backend; any change to storage encodings.

## Compatibility

No public API, protocol, durable record, storage key, journal frame, contract
IR, query IR, module, cursor, or source-language identity changes. Generated
client packages keep their public surface; only their Rust dependency graph
changes. `ApplicationPortabilityManifest` keeps its serialized form when it
moves. Compatibility fixtures remain byte-identical and are the proof that
execution moved without changing meaning.

## Security

Row-policy admission moves from inside the storage transaction to the
executor, which is still first-party Rust below the public boundary. The
executor releases no row, count, measure, or cursor before admission, and
the service still reauthorizes before values cross the public boundary.
Storage stops receiving policy contexts, which removes one place where a
policy value could be misapplied. The client loses transitive access to
compiler and server internals. No trust boundary widens.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** No application-facing surface
  changes. Applications cannot express a query, plan, policy, or storage
  selection they could not express before; the same compiler-sealed programs
  execute in a different crate.
- **Scale:** The decision removes an assumption rather than adding one: an
  executor over narrow readers is the shape a remote or partitioned reader
  needs. The owned-snapshot handle is single-node in V1 and is named as the
  reversible constraint.

## Testing

- `storage_backends_depend_only_on_storage_api_types_proto_and_catalog_driver`
  in `crates/riffdb-storage-redb/tests/architecture.rs`, replacing the
  manifest-string assertions in
  `dependency_surface_keeps_redb_private_and_excludes_infrastructure_assemblies`
  with cargo-metadata resolution in the style of `SIM-007`.
- `executor_owned_snapshot_pages_match_pre_inversion_fixtures`: pages,
  cursors, counts, aggregates, and typed refusals recorded from the current
  in-transaction path for every named-query fixture, replayed through the
  executor over the redb readers.
- `redb_startup_produces_evidence_only_and_drives_no_migration`.
- `client_depends_only_on_proto_tonic_types_errors_and_config` and
  `observability_is_a_leaf_crate`, both cargo-metadata driven.
- `check-crate-graph` with a self-test that injects one upward edge into a
  temporary manifest and expects rejection.
- The existing recovery matrix, ADR-0113 simulation campaign, and
  `tests/migrations` gates run unchanged against redb.

## Requirements and Work Packages

- **Requirements:** `DEP-001` through `DEP-006`
- **Defines or blocks:** WP-754 through WP-756
- **Final evidence:** WP-756

## Decision Deadline

Exact acceptance is required before WP-754 changes any storage-api reader
trait or moves `QueryExecutionPort` out of a concrete backend, and before
WP-755 changes the `riffdb-client-rust` manifest.

## Acceptance

Direction approved 2026-09-01; exact text accepted 2026-09-01. The maintainer
accepted the exact text of this record in the Claude Code session of
2026-09-01, all seven consolidation records together.
