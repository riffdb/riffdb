---
adr: "0203"
title: Selector-Free Offline Storage Scrub Operation
status: proposed
tier: guarantee
date: 2026-09-05
accepted: null
requires: [ADR-0050, ADR-0156, ADR-0157, ADR-0182]
amends: [ADR-0050, ADR-0182]
supersedes: []
requirements: [REC-008]
packages: [WP-760]
obligations:
  - id: OBL-0203-1
    package: WP-760
    proof: public_offline_scrub_drives_the_complete_validation_path
    says: The selector-free public scrub uses the shared authorized offline-maintenance lifecycle, drives the one complete exact-end validation path, persists a bounded compatible receipt, and exposes no validation scope, target, path, backend, repair, force, or readiness selector.
review_triggers:
  - The request or CLI would gain any target, path, namespace, validation-scope, backend, repair, force, skip, readiness, rate, checkpoint, or resume selector.
  - Scrub would bypass the API-neutral service, deny-by-default authorization, quiescence, receipt, exact-end validator, or ordinary post-maintenance readiness proof.
  - Scrub would become an MCP tool or resource, grant application authority, reveal a path, hash, frontier, row count, key, catalog value, or corrupt payload, or mutate authoritative application state.
  - Receipt compatibility, operation identity, phase ordering, failure classification, public field numbering, or the accepted one-incomplete-maintenance-operation rule would change.
---
# ADR-0203: Selector-Free Offline Storage Scrub Operation

## Context

ADR-0182 and REC-008 require the complete exact-end integrity path to become an
authorized public offline scrub. ADR-0050, however, froze named backup and
restore RPCs whose start requests carry backup-specific semantic input. Leaving
the scrub RPC shape to WP-760 would let an implementation package accidentally
invent a target, validation scope, repair mode, or generic maintenance selector
that weakens the public safety boundary.

The scrub has exactly one safe meaning: validate the configured database
completely while ordinary work is quiesced. It neither selects storage nor
repairs it. Its uncertainty identity still has to be caller-stable below the CLI
so retries cannot start duplicate offline work.

## Decision

1. `AdminService` gains exactly one additive unary RPC:

   ```proto
   rpc StartOfflineStorageScrub(StartOfflineStorageScrubRequest)
       returns (StartOfflineStorageScrubResponse);

   message StartOfflineStorageScrubRequest {
     bytes request_id = 1;
     bytes operation_id = 2;
   }

   message StartOfflineStorageScrubResponse {
     OfflineMaintenanceStartDisposition disposition = 1;
     OfflineMaintenanceOperation operation = 2;
   }
   ```

   The request has exactly those two fields. It has no target, database name,
   backup name, path, URI, namespace, table, row class, validation scope,
   backend, repair, force, skip, readiness, rate, checkpoint, resume, or generic
   operation-kind field. Unknown fields retain the existing bounded public-wire
   handling and never select behavior.

2. `OfflineMaintenanceOperationKind` adds the single closed value
   `OFFLINE_MAINTENANCE_OPERATION_KIND_STORAGE_SCRUB = 4`. The caller never
   supplies that enum; the RPC identity fixes it. Existing enum numbers and all
   existing request and response field numbers remain unchanged. In the shared
   `OfflineMaintenanceOperation`, `backup_name` is empty exactly when `kind` is
   `STORAGE_SCRUB`; a nonempty scrub name or an empty name for a backup operation
   is invalid and fails closed.

3. The Rust client exposes one checked `StartOfflineStorageScrub` value built
   only from a caller-stable checked UUIDv7 maintenance operation ID. Each
   transport attempt supplies a fresh checked `RequestId`. The CLI surface is
   exactly `storage.scrub` / `riffdb storage scrub`, with no scrub-specific
   argument or flag; it generates the maintenance operation ID, prints it in
   the existing bounded `riffdb.cli.output/v1` envelope, and uses the existing
   `riffdb backup operation <maintenance-operation-id>` observation command.
   SDK callers retain the same operation ID and exact empty semantic input
   across uncertain retries.

4. Reusing an operation ID for a different maintenance kind, or reusing a scrub
   operation ID with any nonempty semantic input, is the existing stable input-
   mismatch failure and starts no work. A matching retry returns only
   `Accepted`, `AlreadyAccepted`, or the stored terminal observation. Concurrent
   admission retains ADR-0050's one-incomplete-operation rule; scrub adds no
   second queue, worker, lease, writer, or storage handle.

5. gRPC performs only bounded structural decoding and total wire conversion.
   The API-neutral administration service fixes the scrub kind and supplies the
   existing offline-maintenance authorization request. The operation requires
   the same global `AdministerCapabilities` permission, tenant scope, expiry,
   environment, audience, and approval obligations as the accepted ADR-0050
   operations. Confirmation is neither requested nor applicable because scrub
   performs no repair or replacement. CLI and client use only public gRPC; MCP
   gains no tool, resource, schema, visibility, or dispatch path.

6. After durable receipt admission, the existing maintenance controller stops
   admission, drains accepted work under the fixed deadline, stops derived
   workers, closes the authoritative database, and gives the one admitted
   driver the private backend capability. Scrub drives the same complete
   structural and catalog-semantic exact-end validator used by backup
   verification, restore, format upgrade, and retention preflight. It performs
   no repair and no authoritative application mutation. Any malformed,
   incomplete, contradictory, over-limit, or corrupt evidence terminalizes as
   failed closed with an incident-safe failure class; absence and partial
   success are never invented.

7. A successful scrub proves only that this exact closed-database validation
   reached its exact end. It does not create or preserve a clean-close
   certificate, force readiness, suppress or complete the background scrub,
   advance retention, authorize another operation, or make a corrupt row
   readable. The ordinary bounded-root startup validation runs after reopen,
   and readiness remains false until both that proof and the terminal receipt
   are durable. Derived workers restart only afterward.

8. Scrub writes the least new compatible maintenance-receipt version required
   to represent selector-free semantic input and scrub completion. Receipt V1
   and V2 encodings, meanings, filenames, checksums, and readers remain byte-
   frozen. The new receipt retains ADR-0050's checked operation identity,
   canonical input hash, redacted actor/capability and approval identities,
   database identity, bounded monotonic phase history, closed safe failure,
   canonical checksum, atomic replacement, and parent-sync rules. Its canonical
   input hash is domain-separated over the fixed scrub kind and empty input;
   it contains no caller-controlled target or scope. Startup validates and
   reconciles it before readiness under the existing one-incomplete-operation
   bound; an old binary that cannot read the version refuses the maintenance
   subtree before database mutation.

9. The public operation observation remains bounded and closed. It exposes only
   operation ID, closed kind, empty backup name, canonical input hash, phase,
   and safe failure class already carried by `OfflineMaintenanceOperation`. No
   filesystem path, storage key, hash from validated data, frontier, row count,
   catalog value, corrupt bytes, credential, principal data, or incident detail
   crosses the public boundary. Detailed causes remain redacted internal
   tracing correlated by an incident identifier.

10. WP-760 owns the additive Protobuf descriptors and fixtures, API-neutral
    service and policy wiring, receipt compatibility, exact-end reuse, Rust
    client, CLI JSONL goldens, handbook text, crash recovery, and conformance
    proofs. This proposed record authorizes no implementation until a human
    accepts its exact text. It changes no command, contract, MCP, application,
    storage-key, database-format, audit-enum, capability-enum, retention, backup,
    restore, or clean-certificate semantic.

## Options considered

1. **A generic `StartOfflineMaintenance` request with a kind or scope:**
   rejected because it creates a public selector whose future values can bypass
   operation-specific safety and authorization review.
2. **Add scrub fields to a backup start request:** rejected because an empty or
   synthetic backup name obscures semantic identity and invites path coupling.
3. **Expose scrub only in the CLI:** rejected because it would bypass the shared
   public service/client path or require the CLI to own storage authority.
4. **Expose repair or partial-table scrub modes:** rejected; corruption remains
   fail-closed, and repair needs a separate guarantee decision.

## Consequences

- Operators receive one idempotent uncertainty-safe way to request more
  validation, with no way to request less.
- The public Protobuf and external receipt inventories grow additively, so exact
  descriptor, compatibility, redaction, retry, crash, and old-reader-refusal
  fixtures are required.
- Scrub is disruptive by design: it serializes with offline maintenance and
  withholds readiness during close, validation, and reopen.
- Online or partial repair, targeted scrub, resume checkpoints, remote or object
  targets, MCP exposure, and public scrub scheduling remain deferred.

## Standing design tests

- **Interface safety:** every public spelling selects the same configured
  database and complete validator; application code and agents cannot express a
  target, weaker scope, bypass, repair, force, or storage handle.
- **Scale:** requests and receipts are fixed-size and bounded; validation streams
  through the accepted bounded cursor and never builds a population-sized
  index, result, diagnostic, or public response.

## Checks

- `public_offline_scrub_drives_the_complete_validation_path` proves shared
  authorization, quiescence, exact-end reuse, receipt recovery, post-reopen
  readiness, retry identity, and planted-corruption failure.
- Public descriptor, Rust-client, CLI grammar/JSONL, source-span, unknown-field,
  size, redaction, and MCP-exclusion fixtures freeze the selector-free surface.
- Process failpoints cover receipt creation and sync, drain, close, exact-end
  validation, terminalization, reopen, and readiness without duplicate work or
  authoritative mutation.
- Compatibility fixtures prove V1/V2 byte stability, the new receipt's exact
  canonical bytes and bounds, corrupt/unknown-version refusal, and startup
  reconciliation before readiness.
