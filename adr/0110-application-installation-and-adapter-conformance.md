# ADR-0110: Exact Application Installation and Adapter Conformance

- **Status:** Accepted
- **Direction approved:** 2026-08-09
- **Exact text accepted:** Yes, 2026-08-09
- **Decision deadline:** Before WP-568 adds installation operations or a new
  application/conformance manifest format
- **Requires:** ADR-0055, ADR-0057, ADR-0062, ADR-0065, ADR-0066,
  ADR-0075, ADR-0076, ADR-0079, and ADR-0105 through ADR-0109
- **Defines or blocks:** WP-568, WP-569, WP-574, WP-576, WP-578, and WP-579

## Context

The CLI can compose contract deployment, query/reactive module publication,
role reconciliation, credential creation, seed commands, and offline migration.
Adapters still need a programmatic, resumable installation and upgrade surface
that starts from an empty database, creates only the intended initial authority,
proves server/driver features, and cannot leave identity hashes or grants in an
unexplained half-state.

The operation cannot honestly be one database transaction: migration may take
the database offline, credentials live outside the database, and seed commands
are independent business commits. It can be one exact campaign whose stages are
idempotent, observable, and fail closed.

## Proposed Decision

RiffDB adds a versioned **application installation plan** compiled from:

- the exact application source and lock;
- contract, query, reactive, role, and generated artifact identities;
- a declared target database/environment;
- an optional exact migration bundle;
- initial symbolic role bindings and credential destinations;
- bounded seed/import inputs;
- required server and driver features; and
- an adapter-owned conformance manifest.

The compiler emits one content-addressed plan and a bounded safe diff against
the selected database. No remote mutation occurs during planning.

### Programmatic campaign

The shared application service exposes start and observe operations for one
caller-stable installation campaign identity. CLI, Rust operator SDK, and
reviewed deployment controllers use those operations; MCP and stable
application clients do not gain installation authority.

The campaign advances through exact resumable stages:

1. authenticate, select the database, and prove lifecycle/readiness;
2. validate the lock and every local artifact before remote mutation;
3. inspect active contract/module/role identities and feature manifests;
4. deploy a compatible successor or stop with a typed migration requirement;
5. run an explicitly confirmed accepted migration when supplied;
6. publish query and reactive modules;
7. reconcile symbolic roles without implicit authority widening;
8. create successor application credentials at protected destinations;
9. prove the installed identity through the intended driver;
10. run bounded ordinary command seeds/imports; and
11. publish one terminal installation receipt.

Each completed stage is independently verified on resume. The campaign never
reports atomic rollback across stages. Before a stage begins, its mutation and
recovery semantics are those of the existing authoritative operation. A failed
later stage leaves a typed partial campaign listing completed identities and
the exact safe next action.

### Authority and credential rules

Installation requires a dedicated database/environment/application-lineage
permission. Deploy, migrate, administer-capabilities, backup, or application
roles do not individually imply it. The installation proof may internally
consume separately sealed subordinate authorities, but it never synthesizes a
broader capability than the caller can delegate.

Initial roles are symbolic and exact. A role definition widening requires a
bounded public authority diff and explicit approval/confirmation before a new
capability is created. Existing credentials are not silently widened or
overwritten. Rotation follows ADR-0105 and revokes the predecessor only after
successor proof.

An empty database bootstrap retains its principal-less singleton rule.
Installation begins only after an authenticated bootstrap owner or delegated
installer exists; it is not a second bootstrap bypass.

### Evolution and migration

Ordinary compatible deployment, explicit-version activation, and
`RequiresMigration` retain their accepted meanings. The campaign does not
automatically approve destructive migration, suppress backups, skip invalid
predecessor data, translate old writers, or roll back an activated contract.

When migration is required, the plan names the exact parent, successor,
migration hash, confirmation, backup policy, and downtime class. Without all of
them the campaign stops before migration mutation with an actionable typed
result. After successful cutover, module/role/credential/seed stages resume
against the exact active successor.

### Adapter conformance manifest

An adapter repository owns a signed-or-content-addressed manifest declaring:

- adapter and schema version;
- required RiffDB server, driver-host, and generated-client feature IDs;
- exact contract/query/reactive/role inputs or generation rules;
- required bulk, query, workflow, migration, and event capabilities;
- platform/runtime support;
- conformance commands and golden observations; and
- known optional/degraded features.

The manifest cannot request raw permissions, numeric IDs, storage access,
arbitrary commands, shell hooks inside the server, or an unbounded version
range. Installation validates it against the safe symbolic catalog and records
its digest in the receipt. Adapter tests run outside the database process and
may invoke only public operations with scoped credentials.

### Receipts and observations

One bounded installation receipt records the campaign/plan identities, target
database/history, exact installed artifacts, role/capability IDs without bearer
material, migration/backup receipt references, seed checkpoint, conformance
manifest digest, terminal state, and safe remediation codes. It contains no
paths, credentials, submitted seed values, hidden schema, or internal prose.

## Options Considered

1. **Keep a CLI shell script as the canonical installer:** rejected because
   stages, identities, uncertainty, and role widening are not a stable API.
2. **Make the whole installation one transaction:** rejected because external
   credentials, offline migration, and independent seed commands cannot share
   one honest atomic boundary.
3. **Let adapters run arbitrary hooks with operator credentials:** rejected
   because it recreates a privileged plugin system.
4. **A compiled, resumable, exact campaign:** proposed because partial progress
   is explicit and every mutation retains its existing safe owner.

## Consequences

- Compose/Kubernetes controllers and adapters can install and upgrade without
  scraping CLI prose or hardcoding hashes.
- Partial campaigns remain possible but become typed, resumable, and auditable.
- Adapter claims become reproducible feature/conformance evidence.
- General plugin execution, online dual-schema migration, automatic rollback,
  and implicit permission widening remain unavailable.

## Compatibility

Application source/lock, installation plan/receipt, capability permission,
public administration protocol, CLI/operator SDK, and conformance manifest
require additive versioned formats. Existing deploy/migration/role/batch
operations remain valid and are composed, not reinterpreted.

### Implementation correction (2026-08-11)

The first WP-568 implementation incorrectly appended installation authority to
ADR-0089's already-frozen migration-only `CapabilityRecordV2`. The correction
restores V2 source and schema hash byte-for-byte and places installation
authority in the own-file `CapabilityRecordV3` successor. Migration-only grants
remain V2; any grant containing installation authority is V3. The reader also
recognizes the exact interim additive payload emitted under V2's compact
identity and reconstructs its complete authority, while all new writes use V3.
This is the additive-versioning rule this accepted section already required;
it does not widen installation authority or reinterpret an application grant.

## Security

Planning is read-only and redacted. Start/observe authorize current exact
authority. Credential destinations must be protected and disjoint from public
artifacts. Adapter manifests are data, never executable server code. Every
stage repeats database selection and current-policy checks before protected
detail or mutation.

## Standing Design Tests

- **Interface safety:** installers cannot request raw writes, skip compiler
  locks/migration checks, widen roles silently, recover bearer material, or run
  adapter code in the server. A partial campaign cannot masquerade as success.
- **Scale:** campaign metadata is bounded and installation transforms only the
  selected database through existing bounded migration/seed mechanisms. It
  introduces no full-cluster transaction or full-state memory requirement.

## Testing

- Plan/receipt/manifest old-current compatibility and deterministic hash tests.
- Crash/retry at every campaign stage and same-ID/different-plan rejection.
- Role widening, subset delegation, credential destination, rotation, and
  redaction matrices.
- Empty, compatible-upgrade, migration-required, failed-seed, and resume
  acceptance against multiple databases.
- Adapter conformance for OpenFGA, MLflow, Better Auth (Payload deferred
  post-alpha; Amendment 1, 2026-08-11), and Woodpecker shapes in
  Rust, Go, TypeScript, and Python where supported.

## Requirements and Work Packages

- **Provisional requirements:** `APE-001` through `APE-014`, to be added to
  `SPEC.md` only after exact acceptance.
- **Defines or blocks:** WP-568, WP-569, WP-574, WP-576, WP-578, and WP-579.
- **Final evidence:** WP-578 and WP-579.

## Decision Deadline

Exact acceptance is required before an installation permission/RPC, application
format, conformance manifest, role-widening flow, or receipt is implemented.

### Amendment 1: Better Auth replaces Payload in the alpha conformance set (2026-08-11)

- **Status:** Accepted — 2026-08-11, maintainer acceptance as written

Maintainer scope decision: the alpha conformance adapter set is OpenFGA,
MLflow, Better Auth, and Woodpecker. Payload's shape is retained as a named
post-alpha adapter. Better Auth's conformance additionally uses the
framework's own published adapter conformance suite as an external
acceptance instrument, run from the integration repository per ADR-0117.
