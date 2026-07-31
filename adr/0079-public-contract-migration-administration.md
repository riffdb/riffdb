# ADR-0079: Public Contract Migration Administration

- **Status:** Accepted
- **Direction approved:** 2026-07-31
- **Exact text accepted:** 2026-07-31
- **Acceptance reference:** Human maintainer exact-text acceptance in the
  implementation session on 2026-07-31
- **Decision deadline:** Before WP-409 changes capability or Protobuf registries
- **Depends on:** ADR-0076, ADR-0077, ADR-0078
- **Amends:** ADR-0007, ADR-0009, ADR-0021, ADR-0026, ADR-0027, ADR-0028,
  ADR-0034, ADR-0037, ADR-0040, ADR-0041, ADR-0050, ADR-0055, ADR-0062,
  ADR-0063, ADR-0064

## Context

Contract migration is stronger than contract deployment and capability
administration because it transforms current authoritative state and may retire
old writers. Reusing either permission would silently broaden existing roles.
CLI-only direct access would create a privileged path. Exposing migration as MCP
tools would place destructive administration in ordinary agent catalogs and add
no semantic capability beyond the public administrative service.

Migration may run longer than unary deadlines and closes the selected database
graph. The existing offline maintenance model proves caller-stable operation
identity, accepted-then-reconnect behavior, receipt recovery, and no privileged
offline polling channel.

## Proposed Decision

Add durable `CapabilityPermissionV1::MigrateContract`, scoped to one database,
environment, and exact contract lineage. Bootstrap/kernel administration may be
explicitly granted it. `DeployContract`, application roles, ad-hoc query roles,
MCP roles, `AdministerCapabilities`, and backup/restore authority do not imply
it. Policy produces a non-cloneable, nonserializable exact-operation proof with
ordinary approval and redaction obligations and fresh capability-revision
checks before drain.

Add three unary `AdminService` RPCs to the versioned kernel API:

- `CheckContractMigration` starts one read-only complete preflight;
- `ApplyContractMigration` starts one exact migration apply; and
- `GetContractMigrationOperation` observes one caller-stable operation after
  the selected database is available.

The RPC inventory grows from 25 to 28 without modifying existing services,
methods, fields, numbers, or message meanings. The API-neutral service owns all
request validation, authentication/policy input, canonical input hashing,
response budgeting, redaction, and typed results. gRPC maps without semantic
reinterpretation. The Rust SDK exposes operation-specific methods and retry
helpers. The CLI uses only that SDK and adds:

```text
riffdb migration plan --application <path>
riffdb migration check --application <path> --operation-id <uuid>
riffdb migration apply --application <path> --operation-id <uuid> \
  --confirm-apply <64-lowercase-hex-migration-hash>
riffdb migration operation <uuid>
```

This top-level command is distinct from the existing
`riffdb application migrate --to v2`, which changes only local manifest format.
TypeScript, Python, generated application clients, hosted MCP, and stdio MCP add
no migration tool, resource, permission, or hidden call path. MCP authorization
tests must prove migration remains absent even for a credential that has
application or contract-deployment authority.

The durable service-operation registry adds exactly
`ServiceOperationV1::ApplyContractMigration`. A successful final cutover uses
the normal commit-owned control-plane administration sequence and terminal
audit link. Check, observation, and failed/pre-cutover apply use the external
migration receipt as the same narrow offline-maintenance audit exception as
ADR-0050 and assign no administration sequence. They do not masquerade as
contract deployment or capability administration.

Introduce domain-specific caller-stable UUIDv7 `ContractMigrationOperationId`,
`MigrationBundleHash`, and `ContractMigrationInputHash`. Start retries use a
fresh transport `RequestId` but the same operation ID and canonical semantic
input. Same ID/same input returns the existing observation; same ID/different
input fails before drain. A different operation ID for an already-active exact
successor returns typed `AlreadyApplied` only after the permanent migration
record and artifact identities agree.

Apply additionally requires exact wire confirmation
`ALLOW_APPLY_CONTRACT_MIGRATION` and the complete migration bundle hash. CLI
emits both only from `--confirm-apply <hash>`. Confirmation proves operator
intent, not authority. Check requires no confirmation and never creates a
backup or stage.

Start returns a durable `Accepted` observation and operation ID before the
database enters the offline interval. After acceptance, transport cancellation
or deadline does not cancel or roll back work. While the selected database is
offline, its ordinary authentication graph and operation polling remain
unavailable; `--wait` reconnects with bounded backoff until the database reopens
and then reads the receipt. Sibling databases remain usable through their own
selectors. No frozen-auth or unauthenticated progress endpoint is added.

Closed operation phases are `Accepted`, `Draining`, `Preflight`,
`BackupPublished`, `Staging`, `Transforming`, `RebuildingProjections`,
`ValidatingStage`, `Publishing`, `ValidatingPublished`, `RollingBack`,
`Succeeded`, `FailedClosed`, and `FailedRolledBack`. Public observations contain
exact artifact hashes, safe counts, optional retained backup name/hash, current
phase, and bounded static failure/repair codes. They never contain row values,
credentials, paths, dependency prose, or internal errors.

Application deployment encountering `RequiresMigration` returns the exact
candidate descriptor and required migration identities without remote mutation.
After migration success, the operator reruns `riffdb application deploy`; it
resumes query-module and role reconciliation against the already-active exact
candidate. Application admission remains fail closed in `DeploymentRequired`
until that campaign completes.

## Options Considered

1. **Reuse DeployContract:** Rejected because source-author authority would gain
   data transformation and old-writer retirement.
2. **Reuse broad kernel administration:** Rejected because it prevents least-
   authority delegation and obscures audit intent.
3. **Add MCP tools:** Rejected for the first release because migration is an
   operator workflow and MCP must not gain a privileged path.
4. **gRPC, Rust SDK, and CLI with a dedicated permission:** Proposed because it
   preserves the shared service boundary and least authority.

## Consequences

- Capability, public Protobuf, SDK, CLI JSON, and generated inventories gain
  additive pre-alpha variants and fixtures.
- Long operations are observable only after reconnect in the first release.
- Applications cannot combine deployment and migration into one implicit
  command; the safe interruption boundary is explicit and resumable.

## Compatibility

Existing permission tags, RPCs, messages, SDK methods, CLI commands, MCP names,
and application drivers remain unchanged. New tags and methods require exact
versioned fixtures. Unknown confirmation, operation kind, phase, or result fails
closed.

## Security

Database selection precedes credential lookup. Authentication and policy run
through existing shared boundaries. Start, retry, poll, and already-applied
paths all authorize current authority before returning operation-specific
details. Redaction occurs before protocol conversion, logs, metrics, or public
diagnostics.

## Testing

- Protobuf descriptor/wire, SDK, CLI JSONL, help, and generated reference
  fixtures.
- Authorization matrices for wrong database, lineage, environment, permission,
  capability revision, confirmation, hash, and operation identity.
- Negative MCP discovery/invocation/resource tests.
- Cancellation, uncertainty, duplicate-start, reconnect, sibling-database, and
  application-deployment resume tests.
- Response-budget and redaction tests for every operation phase and failure.

## Requirements and Work Packages

- **Requirements:** `MIG-006` through `MIG-010`, `MIG-016` through `MIG-018`
- **Defines or blocks:** `WP-409`, `WP-410`, `WP-413`
- **Final evidence:** `WP-413`

## Decision Deadline

Exact human acceptance is required before WP-409 changes the durable capability
registry, public Protobuf inventory, API-neutral service, SDK, or CLI.
