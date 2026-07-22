# ADR-0026: WP-120 Authorization, Recovery, and Failure Boundaries

- **Status:** Accepted
- **Direction approved:** 2026-07-21
- **Exact text accepted:** 2026-07-21, amended 2026-07-22
- **Accepted:** 2026-07-21
- **Requires:** ADR-0005, ADR-0007, ADR-0009, ADR-0012, ADR-0018, and
  ADR-0023
- **Clarifies:** Grammar-v1 tenant scope, outcome-recovery authorization,
  committed-outcome durability presentation, and incident-source failure at the
  WP-120 service boundary
- **Amended by:** ADR-0040 for the checked outcome-locator selector and its
  unchanged two-phase authorization boundary
- **Decision deadline:** Before WP-120 publishes its command-service
  implementation

The human maintainer accepted these narrow interface rules on 2026-07-21. This
record adds no tenant-mapping language, service operation, public error kind,
public Protobuf field, durable field, storage key, or durability mode.

## Context

The accepted architecture fixes four adjacent behaviors but does not yet give
WP-120 a complete Rust interface for them.

First, SPEC says grammar v1 has no tenant mapping and commands are authorized
only under global tenant scope. Command idempotency inspection needs that exact
scope before a historical plan has been selected, while the current policy
value is only a zero-sized marker with no checked scope accessor.

Second, ADR-0007 requires outcome recovery authorization before lookup and again
before returning a complete historical outcome. Before lookup, the service has
only authenticated request identity, lineage, command ID, and grammar-v1's
static tenant rule. It must not invent a contract version, owner, partition, or
outcome fact merely to satisfy the full post-lookup policy request.

Third, the commit-owned `CommittedOutcome` retains the exact storage outcome,
including its original durability. WP-120 must map that fact to its
storage-neutral service DTO without depending on `riffdb-storage-api` or
presenting test-only memory execution as durable production success.

Finally, ADR-0018 forbids a fallback incident ID when `IncidentIdSource` fails.
ADR-0007's ordinary internal `PublicError` requires a real incident ID. The
service therefore needs a closed emergency disposition that can withhold a
protected result without fabricating a correlation value or leaking the
original failure.

## Decision

### Grammar-v1 operation tenant proof

`riffdb-policy` owns `OperationTenantScope` as a fields-private checked value,
not a raw tenant selector and not an authorization allow decision. Its only
command-semantic constructor in the POC is `grammar_v1_global()`, and its
read-only accessor returns exactly `TenantScope::Global`. The existing
`global_only()` constructor remains an equivalent compatibility spelling for
current POC data-read schemas; command-service code uses the grammar-specific
spelling.

The current authorizer compares a capability grant's tenant scope with the exact
scope retained by this value. It does not interpret every present
`OperationTenantScope` as implicitly global. The execute-command and
outcome-recovery request constructors retain the grammar-v1 global proof. The
service may use the same policy-owned value to construct the tenant component of
the pre-lookup idempotency identity; it may not accept a caller-supplied tenant
or derive one from input data.

This value proves only the static grammar rule. Current capability resolution,
exact command permission, owner equality, partition permission, approval, and
all other obligations still require a fresh `CurrentAuthorizer` allow decision.
A tenant-mapped grammar construct, contract annotation, arbitrary tenant
constructor, or compiler-to-policy lowering requires a separately accepted ADR
and is outside the POC.

### Two-phase outcome-recovery authorization

`riffdb-policy::OperationRequest` has a distinct
`resolve_command_outcome_pre_lookup(lineage, command_id)` constructor. It maps
to the unchanged `ResolveCommandOutcome` service-operation tag, requires the
exact `InvokeCommand(lineage, command_id)` permission and grammar-v1 global
tenant scope, and contains no contract version, recorded owner, recorded
tenant, partition, result, or durable record.

An allow decision for this request authorizes only the bounded internal outcome
lookup under the authenticated stable principal, grammar-v1 tenant scope, and
caller-provided idempotency identity material. It is not authority to return a
present protected outcome and is not cached as the second safe-point proof. A
missing result may be reported with the accepted bounded absence semantics
because no protected outcome exists to disclose.

When lookup returns a complete outcome, the service constructs the existing full
`resolve_command_outcome` policy request from the exact stored contract version,
command identity, owner principal, owner tenant, and recorded partition. It then
reloads current capability state, samples fresh authorization time, and obtains
a second allow decision immediately before shaping and returning the complete
outcome. Exact command permission represents permission for the command's
complete declared outcome schema; the service never returns a partial outcome.

The full check must prove the current stable principal and effective tenant equal
the stored owner, the current capability still invokes that exact command, and
its partition authority includes the exact recorded partition. A rotated
capability is allowed only when all those current facts pass. Lookup failure,
malformed or incomplete returned facts, current-policy failure, or inability to
perform the second check withholds the outcome and fails closed.

### Storage-neutral committed durability

`riffdb-commit::CommittedOutcome` exposes a `durability()` accessor returning
`Result<CoordinatorDurability, CommittedOutcomeDurabilityError>`. It maps the
exact original stored `Sync` and `Group` modes to the existing commit-owned
production enum. It maps test-only `Memory` to the closed error. Neither the
accessor nor its error exposes `riffdb-storage-api::DurabilityMode` or another
storage DTO.

WP-120 maps `CoordinatorDurability::Sync` and `Group` mechanically to its
service-owned durability DTO. The error is an internal invariant failure at a
production service boundary; it cannot be relabeled as sync or group and no
journaled success may be released. Replay reports the original commit mode, not
the coordinator's current configuration.

### Incident-source emergency failure

`riffdb-errors` owns `EmergencyInternalFailure`, constructible from
`IncidentIdSourceError`. It is a closed, non-diagnostic marker with a fixed
generic safe message, internal error class, and operator recovery guidance. It
contains no `IncidentId`, arbitrary message, original protected result, or error
source, and there is deliberately no conversion from it to `PublicError`.

WP-120 adds a distinct closed `ServiceFailure` disposition for this value. When
an internal failure requires containment and the injected incident source also
fails, the service discards or withholds the protected output and returns that
emergency disposition. It does not construct `PublicError::internal_defect`,
reuse an earlier ID, derive an ID from request data, emit an all-zero/counter ID,
or attach internal diagnostics.

Transport adapters map the emergency disposition to a generic internal failure
without structured `PublicError` details or a claimed incident correlation ID.
They may emit only bounded static emergency telemetry. Production composition
marks the incident-source condition as readiness-failing. Recovery of the
source affects later requests only; it does not retroactively invent an ID for
the failed response.

## Options Considered

1. **Policy-owned grammar-v1 global proof:** accepted; it makes the static
   idempotency/authorization scope explicit without adding tenant mapping.
2. **Use a caller-supplied `TenantScope`:** rejected; it lets a request choose a
   semantic identity component that grammar v1 fixes globally.
3. **Authorize outcome lookup with fabricated placeholder facts:** rejected;
   placeholder version, owner, or partition values could deny valid recovery or
   authorize disclosure against facts that were never stored.
4. **Perform only the post-lookup check:** rejected; lookup itself is a protected
   operation and ADR-0007 explicitly requires both safe points.
5. **Expose storage durability directly to WP-120:** rejected; it violates the
   consumer boundary and permits accidental treatment of memory mode as a
   production guarantee.
6. **Map memory mode to sync, group, or absence:** rejected; each mapping makes a
   false durability claim.
7. **Fabricate a fallback incident ID:** rejected by ADR-0018; it creates a false
   correlation claim and can make request data part of an internal identifier.
8. **Return an ordinary internal `PublicError` without its required incident
   ID:** rejected; it weakens the accepted public-error invariant.

## Consequences

- WP-120 can form the grammar-v1 idempotency identity before plan selection
  without accepting a tenant assertion or importing compiler internals.
- Outcome recovery has two representable policy requests and cannot satisfy the
  first safe point by guessing durable facts.
- A pre-lookup allow proof is deliberately weaker than a full outcome-disclosure
  allow proof; reviewers must verify that no returned outcome is released
  between them.
- Service result mapping no longer needs to inspect a storage durability type,
  and memory-backed production composition fails visibly and closed.
- Incident-source failure has a representable service path without expanding or
  weakening `PublicError`.
- Future tenant mapping, a new durability mode, or a public emergency-error wire
  shape requires a new compatibility and security decision.

## Compatibility

This decision changes additive internal Rust interfaces and the private
representation of one policy value. It leaves the 22 service operations and all
stable operation tags unchanged. It changes no contract grammar/IR, plan hash,
canonical input hash, idempotency identity encoding, capability permission,
public Protobuf message, durable Protobuf envelope, durable record, storage key,
commit order, or atomicity rule.

`OperationTenantScope::global_only()` remains source-compatible. The new
pre-lookup request and committed-outcome/error accessors are additive. The
emergency marker is intentionally outside the public-error compatibility
surface; adapters expose no new structured error code or detail.

## Security

All four decisions are fail closed. Static tenant scope cannot be selected by
the caller. Pre-lookup authorization grants no disclosure authority and the
second authorization is based only on exact returned facts. Storage memory mode
cannot masquerade as durable success. Incident-source failure withholds
protected output and neither invents correlation data nor forwards diagnostic
text to logs, metrics, transport details, MCP content, or CLI output.

MCP, gRPC, CLI, SDK, and in-process comparison paths consume the same service
behavior. This record gives no transport direct policy, commit, or storage
access and creates no MCP privilege exception.

## Testing

`riffdb-policy` unit tests freeze the grammar-v1 scope accessor, exact tenant
comparison, unchanged service-operation tag, exact pre-lookup command
permission, and absence of owner/partition requirements before lookup. Policy
tests prove the pre-lookup check can allow only under current global command
authority and that the full request separately enforces stored owner, tenant,
and partition facts.

`riffdb-commit` unit tests freeze sync/group mapping, replay preservation, and
memory-mode rejection without a storage type in the public accessor signature.
`riffdb-errors` tests freeze generic formatting, internal classification,
operator recovery, absent source, and the lack of a `PublicError` conversion.

WP-120 integration tests must exercise current-policy change between the two
outcome checks, wrong stored owner/tenant/partition, missing outcome, storage
failure, memory-mode injection, incident-source failure, protected-output
withholding, and absence of a fabricated incident ID. WP-130 process tests must
prove production readiness fails when incident generation is unavailable and
that real sync outcomes retain their exact durability after restart and replay.
WP-200 supplies final cross-transport non-bypass and redaction evidence.

## Requirements and Work Packages

- **Requirements:** `SYS-004`, `API-001`, `ID-005`, `TXN-040`, `TXN-041`,
  `OUT-003`, `POC-008`, `MCP-046`
- **Defines or blocks:** the narrow prerequisite interface slice in
  `riffdb-policy`, `riffdb-commit`, and `riffdb-errors`; `WP-120`; and the
  corresponding `WP-130` adapter mappings
- **Final evidence:** `WP-130` for the production process boundary and `WP-200`
  for cross-transport authorization and redaction

## 2026-07-22 outcome-locator selector amendment

ADR-0040 extends `ResolveCommandOutcomeRequest` with a fields-private checked
selector having exactly `RawKey { lineage, source_command, idempotency_key }`
and `Locator(OutcomeResourceLocator)` variants. Both selectors retain the same
`ResolveCommandOutcome` operation and audit tag, the existing existence-blind
initial authorization, authoritative point lookup, historical-plan validation,
terminal authorization, disclosure, and single terminal audit lifecycle.

The raw branch preserves its active-catalog command resolution. The locator
branch uses the locator's checked lineage and stable command ID for the initial
check, requires the locator owner to equal the authenticated principal, never
substitutes the active contract, and verifies the returned historical command
and compiler-owned tool name before release. After initial authorization,
principal, digest-inventory, existence, lineage, command, or tool mismatch is
the same nondisclosing `NotFound`; malformed public syntax rejects before
lookup. The locator grants no authority and neither branch bypasses the second
authorization check.

## Decision Deadline

This exact text is accepted before WP-120 publishes command execution and
outcome-recovery orchestration. The owner-crate interfaces land first so WP-120
does not duplicate policy scope, inspect storage DTOs, fabricate lookup facts,
or weaken internal-error containment while implementing the shared service.
