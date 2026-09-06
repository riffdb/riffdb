---
adr: "0203"
title: Selector-Free Offline Storage Scrub Operation
status: proposed
tier: guarantee
date: 2026-09-05
accepted: null
requires: [ADR-0050, ADR-0156, ADR-0157, ADR-0179, ADR-0182, ADR-0199]
amends:
  - ADR-0050 for the additive selector-free storage-scrub operation
  - ADR-0179 for generated audit-exempt maintenance declarations
  - ADR-0182 for the exact public health encoding and REC-004 reconciliation
  - ADR-0199 for the one closed non-PERF WP-760 definition correction
  - REC-004
supersedes: []
requirements: [REC-004, REC-007, REC-008]
packages: [WP-760]
obligations:
  - id: OBL-0203-1
    package: WP-760
    proof: public_offline_scrub_drives_the_complete_validation_path
    says: The selector-free public scrub uses the shared authorized offline-maintenance lifecycle, drives the one complete exact-end validation path, persists a bounded compatible receipt, and exposes no validation scope, target, path, backend, repair, force, or readiness selector.
  - id: OBL-0203-2
    package: WP-760
    proof: every_public_operation_has_exactly_one_registry_declaration
    says: Each of the five public receipted-maintenance RPCs has exactly one generated, audit-exempt, gRPC-only registry declaration separate from durable ServiceOperationV1.
review_triggers:
  - The scrub request, client, or CLI would gain a target, path, namespace, scope, backend, repair, force, skip, readiness, rate, checkpoint, resume, or kind selector.
  - Scrub would bypass the API-neutral service, policy authorization, quiescence, receipt, exact-end validator, or ordinary post-maintenance readiness proof.
  - Registry coverage, gRPC-only ingress, health numbering, state validity, incident exposure, aggregate classification, receipt phase ordering, failure classification, operation identity, or the one-incomplete-operation rule would change.
  - The WP-760 correction would change anything except required_adrs and the seven named allowed-path families, change PERF authority, or lift the freeze.
---
# ADR-0203: Selector-Free Offline Storage Scrub Operation

## Context

ADR-0182 requires complete validation to move off dirty readiness and into first
access, one background scrub, and one authorized public offline scrub. ADR-0050
does not freeze that scrub's wire shape, and current health wire types cannot
encode the scrub's five states. Implementation must not invent a selector or an
ambiguous status mapping.

WP-760 also predates this record: its definition omits this required ADR and the
seven public-interface path families the accepted deliverables necessarily touch.
ADR-0199 otherwise seals that definition while the performance freeze is in
force. Finally, SPEC REC-004 still says every ineligible clean certificate runs
the complete exact-end path, although accepted ADR-0182 replaced that rule with
bounded dirty recovery.

## Decision

1. `AdminService` gains exactly this additive unary RPC and messages:

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

   The request has exactly those two fields. Unknown fields retain the bounded
   public-wire refusal rules and never select behavior. It has no target,
   database, path, URI, namespace, table, validation scope, backend, repair,
   force, skip, readiness, rate, checkpoint, resume, or operation-kind field.

2. ADR-0179's registry coverage includes every public receipted-maintenance RPC:
   `CreateOfflineBackup`, `RestoreOfflineBackup`, `RetireOfflineBackup`,
   `GetOfflineMaintenanceOperation`, and `StartOfflineStorageScrub`. Each has
   exactly one closed `MaintenanceOperationDeclaration`, keyed only by its fully
   qualified service/method identity and distinct from `ServiceOperationV1`.
   It names its Protobuf/DTO pair, bounds, field map, permission, idempotency,
   redaction, and audience; generators emit request/response validation and
   routing metadata from it. Each declaration admits exactly
   `ServiceIngressKindV1::Grpc`; CLI is a gRPC client.
   Neither `McpHttp` nor in-process comparison is a public ingress. This is not
   an exemption from ADR-0179 generation or coverage. Per ADR-0050 it adds no
   durable audit operation or tag; the receipt remains the sole audit record.

3. `OfflineMaintenanceOperationKind` adds only
   `OFFLINE_MAINTENANCE_OPERATION_KIND_STORAGE_SCRUB = 4`. Existing enum and
   field numbers remain unchanged. The RPC, never its caller, fixes the kind.
   `OfflineMaintenanceOperation.backup_name` is empty exactly for scrub and is
   nonempty for every backup kind; any other combination fails closed.

4. The authenticated health schema grows additively as follows; existing enums,
   fields, and numbers remain unchanged:

   ```proto
   enum StorageScrubHealthState {
     STORAGE_SCRUB_HEALTH_STATE_UNSPECIFIED = 0;
     STORAGE_SCRUB_HEALTH_STATE_PENDING = 1;
     STORAGE_SCRUB_HEALTH_STATE_RUNNING = 2;
     STORAGE_SCRUB_HEALTH_STATE_COMPLETE = 3;
     STORAGE_SCRUB_HEALTH_STATE_FAILED = 4;
     STORAGE_SCRUB_HEALTH_STATE_CANCELLED = 5;
   }
   message StorageScrubHealth {
     StorageScrubHealthState state = 1;
     optional bytes incident_id = 2;
   }
   message AuthenticatedHealth {
     // existing fields 1..7 unchanged
     optional StorageScrubHealth storage_scrub = 8;
   }
   ```

   ADR-0182's `scrub` component means exactly this dedicated member beside the
   existing bounded `components` list; no generic `HealthComponentKind` or
   `HealthComponentStatus` value is added. It is present in authenticated gRPC
   health and has exactly one non-`UNSPECIFIED` state. `incident_id` is absent
   except in `FAILED`, where it is one checked 16-byte server-generated UUIDv7
   `IncidentId`. The process-local machine starts `PENDING`; only
   `PENDING -> RUNNING|CANCELLED` and
   `RUNNING -> COMPLETE|FAILED|CANCELLED` are valid, terminal states do not
   transition, and restart begins again at `PENDING`. No other state/incident
   combination is valid.

5. A scrub failure also sets the existing `AUTHORITATIVE_STORAGE` component to
   `DEGRADED`; together with the scrub member's incident ID this is ADR-0182's
   “degrades Storage with an incident identifier” rule. That exact combination
   yields aggregate `HEALTH_STATUS_DEGRADED` while serving continues and corrupt
   rows fail closed on access. Existing authoritative unavailability still
   yields `NOT_READY`. Pending, running, complete, and ordinary ready operation
   yield `READY`; cancellation exists only during shutdown. Health exposes no
   incident detail, diagnostic text, hash, frontier, count, path, key, catalog
   value, or corrupt bytes.

6. The Rust client accepts only a caller-stable checked UUIDv7 maintenance
   operation ID and supplies a fresh checked `RequestId` per transport attempt.
   The CLI is exactly `riffdb storage scrub` (operation spelling
   `storage.scrub`), has no scrub-specific argument or flag, generates the
   operation ID, and prints it in the bounded `riffdb.cli.output/v1` envelope.
   Observation reuses `riffdb backup operation <maintenance-operation-id>`.
   MCP gains no tool, resource, schema, visibility, or dispatch path.

7. Matching uncertain retries return only `Accepted`, `AlreadyAccepted`, or
   the stored terminal observation. Reusing an operation ID for another kind or
   nonempty scrub semantic input is the stable input-mismatch failure. Scrub
   uses ADR-0050's one-incomplete-operation admission, API-neutral service,
   global `AdministerCapabilities` authorization, controller, and receipt
   recovery. It adds no queue, writer, lease, storage handle, or application
   authority.

8. After durable admission, the controller stops admission, drains accepted
   work under the fixed deadline, stops derived workers, closes the database,
   and gives one private backend capability to the scrub driver. That driver
   streams the shared complete structural and catalog-semantic exact-end
   validator used by backup verification, restore, upgrade, and retention
   preflight. It never repairs or mutates application state. Any incomplete,
   contradictory, corrupt, or over-limit evidence fails closed.

9. Success proves only this closed-database pass reached exact end. It creates
   no clean certificate, forces no readiness, suppresses no background scrub,
   and advances no retention state. Reopen runs bounded-root startup validation;
   readiness waits for that proof and the durable terminal receipt. Only scrub
   writes maintenance receipt V3; create/restore remain V1 and retire remains
   V2, whose bytes and meanings stay frozen. V3 retains ADR-0050's identity,
   bounded phase history, redaction, checksum, atomic replacement, parent sync,
   recovery, and unknown-version refusal, and adds only scrub kind plus explicit
   absent backup input. Its sole success sequence is exactly
   `Accepted -> Draining -> Offline -> Validating -> Succeeded`;
   `FailedClosed` is reachable from any nonterminal state, `ArtifactPublished`
   is never valid for scrub, and no other transition or re-entry is permitted.
   Its input hash is domain-separated over fixed scrub kind and empty input.

10. For REC-004, accepted ADR-0182 section 1 replaces only its stale first
   sentence: startup with any missing, consumed, malformed, stale, repaired,
   migrated, restored, or otherwise ineligible clean-close certificate executes
   engine/format checks, complete journal-suffix recovery, bounded-root
   validation, dirty-generation consumption, then readiness, with no population
   walk. The complete exact-end path is required by explicit scrub and the
   already accepted offline validation call sites, not dirty readiness. The
   remaining REC-004 durability, ordering, and no-advance requirements are
   unchanged. WP-760 updates the authoritative SPEC wording accordingly.

11. This record makes one closed non-PERF amendment to ADR-0199 Decision 3.
    While the freeze remains in force, WP-760 may change its normalized
    definition only by adding `ADR-0203` to `required_adrs` and adding exactly
    `proto/**`, `crates/riffdb-proto/**`, `crates/riffdb-api-grpc/**`,
    `crates/riffdb-client-rust/**`, `crates/riffdb-policy/**`,
    `crates/riffdb-types/**`, and `crates/riffdb-operation-registry/**` to
    `allowed_paths`. Its requirements, objective,
    deliverables, acceptance, exit gate, design tests, review triggers, PERF
    requirements/evidence/thresholds, and all other fields remain byte-exact
    apart from YAML formatting forced by those two additions. This authorizes no
    candidate, new performance work, fifth exception, or freeze lift.

12. If accepted, the companion reviewed package-metadata change applies exactly
    Decision 11 to `work_packages.yaml`; it makes no closure change. WP-760 then
    owns the registry, descriptors/fixtures, API-neutral DTO and service, policy, gRPC,
    Rust client, CLI, health classification, receipt, docs, recovery, and tests.
    This proposed record authorizes no implementation before exact-text human
    acceptance.

## Consequences

- Operators gain one idempotent way to request more validation and no way to
  request less; old wire readers ignore the additive health member.
- Background scrub state and its one safe incident correlation become closed,
  bounded, and testable; no free-form diagnostic crosses the public boundary.
- Scrub is disruptive and serializes with offline maintenance. Targeted scrub,
  repair, resume, remote targets, scheduling controls, and MCP remain deferred.

## Standing design tests

- **Interface safety:** no application or agent can choose storage, scope,
  target, weaker validation, bypass, repair, readiness, scrub suppression, or a
  storage handle. The operator operation only performs the one complete pass;
  health is observation-only and closed.
- **Scale:** requests and health entries are fixed-size; receipts are bounded;
  complete validation streams through the accepted bounded cursor and never
  builds a population-sized index, result, diagnostic, or response.

## Checks

- `public_offline_scrub_drives_the_complete_validation_path` proves shared
  authorization, quiescence, exact-end reuse, receipt recovery, readiness,
  retry identity, and planted-corruption failure.
- `background_scrub_health_transitions_are_closed_and_cancellable` freezes all
  state/incident combinations, Storage and aggregate classification, bounds,
  rate, and cancellation; descriptor, conversion, redaction, and MCP-exclusion
  fixtures freeze the additive wire.
- Process failpoints cover receipt sync, drain, close, validation,
  terminalization, reopen, and readiness; compatibility fixtures prove V1/V2
  byte stability, new receipt bytes, corrupt/unknown-version refusal, and
  startup reconciliation.
