# ADR-0119: Compiler-Owned Workflow Reconstitution for Portable Reimport

- **Status:** Proposed
- **Direction approved:** Not yet
- **Exact text accepted:** No
- **Decision deadline:** Before WP-599 changes workflow grammar/IR, capability
  formats, installation ordering, or the public reimport protocol
- **Requires:** ADR-0003, ADR-0005, ADR-0055, ADR-0072, ADR-0080,
  ADR-0106, ADR-0107, ADR-0109, ADR-0110, ADR-0111, ADR-0112,
  ADR-0115, and ADR-0118
- **Amends if accepted:** ADR-0109's creation boundary and ADR-0112's
  compiler-owned reimport mechanism
- **Defines or blocks:** WP-575, WP-578, WP-579, and WP-599

## Context

WP-575 proved that current entity state can be exported symbolically and that
ordinary bulk create commands can safely reimport OpenFGA- and Payload-shaped
rows. The same mechanism cannot honestly reconstitute MLflow and Woodpecker
workflow entities.

The workflow compiler correctly rejects `set row.state = imported.state`:
ordinary commands may change a workflow state only through a declared legal
transition with an exact observed revision. Omitting the assignment also
fails because a create binding must definitely initialize its complete record.
Lease owner, expiry, fence, and attempt state introduce an additional hazard:
blindly restoring a live lease could authorize a stale worker, while resetting
its fence could reuse a fencing token.

Mapping these entities to a raw import API would violate the product's central
rule that application state changes only through compiler-owned behavior. A
normal command that could arbitrarily select an initial workflow state would
also turn portability authority into an application-visible state-machine
bypass. The all-domain `EXP-014` gate therefore needs a distinct, closed
reconstitution operation rather than a relaxed `set` rule.

## Proposed Decision

### Reconstitution is a distinct compiled-command class

The contract language adds a `reimport command` declaration. It is lowered to
versioned command IR and evaluated by the deterministic command runtime; the
commit coordinator remains the only component that assigns sequences or
applies authoritative mutations. State, typed outcome, provenance,
idempotency, and commit evidence remain atomic for each invocation.

A reimport command is create-only. Its source must be one exact exported
entity record or a statically bounded list of one exact record type. The
compiler derives the complete primary key, field construction, aggregate,
conflict, invariant, relationship, uniqueness, index, row-policy, and
workflow dependencies from the contract. Source cannot name field IDs,
storage keys, callbacks, predicates, expressions, tables, transactions, or a
partial record. Reimport commands cannot mutate or delete an existing entity,
emit arbitrary historical events, call another command, or access storage.

The normal command service, generated application clients, application MCP,
and ordinary roles cannot invoke or even advertise this command class. It is
available only inside one exact application-reimport campaign through a
separate operator surface and authority.

### Workflow state is preserved; live leases are not portable

For a workflow entity, the compiler permits reconstitution of its declared
state field only inside the reimport command's create construction. The value
must come from the exact complete exported record and must be a variant in the
destination workflow state enum. This is not a transition and cannot target an
existing row.

Before a workflow record is accepted, the reimport coordinator proves that
every declared lease is quiescent in the exported snapshot: owner and expiry
are both null. A snapshot containing an owned or partially cleared lease is
valid application data but is not reimportable through this plan. The proof is
first made while the source database is still available: an export started
with portability intent binds the exact portability manifest, evaluates every
selected workflow record at the export snapshot, and records canonical
per-workflow lease-quiescence evidence in its receipt. If any selected record
has an owner, an expiry without an owner, or any other partially cleared lease,
the export terminates with typed `WorkflowNotQuiescent`, names only the
workflow symbol and bounded counts, publishes no completed portability receipt,
and directs the operator to release/expire work and start a new export. It
never silently clears or rewrites source state.

A general export may still carry non-quiescent workflow data for inspection or
an independently authorized archive, but it cannot later be relabeled as a
portability-intent export and its receipt is not accepted by reimport. Reimport
requires the exact completed portability-intent receipt and revalidates each
record against its quiescence evidence before mutation. This second check
detects corruption or substitution; it is not the first point at which an
operator discovers that the source must be changed.

The destination preserves the exported workflow state, fencing token, and
attempt count exactly and preserves the null owner/expiry pair. Every first and
subsequent successful destination claim must mint, by checked arithmetic, a
fencing token strictly greater than the preserved current token; exhaustion
selects the declared exhausted outcome with no mutation. Preserving the fence
together with this monotonicity rule prevents token reuse. Entity revision
restarts at the destination's first revision and the receipt declares that
boundary. Runtime credentials from the source database are never installed in
the destination, and new application credentials are not published until
reimport reconciliation is complete, so a stale source worker cannot present
old revision/fence evidence to the destination.

### One bounded, resumable reimport campaign owns publication

Reimport extends the exact installation campaign as an optional stage between
role reconciliation and credential publication. A normal empty install records
`reimport_not_required`; a portability install records the exact manifest and
campaign evidence. It requires:

- one empty, not-ready destination database with a new database identity;
- the exact installed contract/query/row-policy artifacts;
- a canonical completed portability-intent export manifest and receipt whose
  per-workflow evidence proves lease quiescence at the bound snapshot;
- the exact adapter-owned portability manifest; and
- a caller-stable UUIDv7 campaign identity.

The compiler derives a finite entity-type schedule from required relationship
dependencies. Parent types precede dependent types; cycles, self-references
that cannot be satisfied incrementally, and unsupported event reconstruction
fail plan compilation and require an accepted application migration or an
explicit portability omission. The client cannot choose ordering.

Input is consumed as bounded canonical pages. Each page and record hash is
checked against the completed export before evaluation. The server derives
each idempotency identity from the export-manifest hash, portability-manifest
hash, record class/symbol, and canonical stable entity key. Callers never
supply retry identities. A same-identity retry returns the stored outcome; a
different record under the same identity fails before mutation.

The campaign is not falsely atomic across all records. It records a durable
checkpoint and typed per-mapping outcomes after ordinary atomic command
commits. Crash/restart resumes the exact campaign. Cancellation or a terminal
business/validation failure leaves the destination in a typed, not-ready
partial state; it cannot serve application traffic or publish runtime
credentials. Recovery is resume with identical artifacts or destroy the
unpublished destination through the existing confirmed maintenance path.

After all mappings finish, the service executes the manifest's bounded named
observations, verifies counts/hashes/declared omissions, seals the canonical
reimport receipt, performs full startup-equivalent validation, and only then
publishes readiness and creates/rotates destination application credentials.

### Authority and language surfaces remain separated

A Capability V7 application-reimport grant is distinct from application command,
deployment, migration, installation, export, backup, and capability-
administration authority. It binds one database, environment, application
lineage, exact portability manifest, and principal/whole-application scope.
The capability successor is additive, canonical, bounded, narrowing-only on
delegation, fail-closed on old binaries, and covered by migration fixtures.
WP-599 must verify at its merge base that V7 remains the next unused capability
successor after WP-597's `CapabilityRecordV6`; if another accepted package has
occupied that slot, implementation stops and this ADR is amended before any
format is emitted.

The contract must declare one exact reimport role whose compiled row policy
permits precisely the records the portability manifest names. Whole-application
reimport does not silently disable row policy; the dedicated role makes its
intended scope explicit and reviewable.

The public surface provides start/page/status/cancel through the shared
application service, TLS gRPC, Rust operator client, CLI, and the Rust-owned
driver host. Go, TypeScript, and Python operator bindings may use that host but
do not implement transport trust or gain reimport through normal application
clients. All public errors and receipts are typed, bounded, value-redacted,
and safe for agents. Secret-classified values remain full fidelity inside the
authorized record carriage but never appear in diagnostics, progress, or
receipts.

### Historical events remain explicit

Reconstituting current entity state does not synthesize historical domain
events, provenance, public audit, commit sequences, or entity revisions.
Adapters must map an event to a compiler-supported deterministic command or
declare it omitted exactly as ADR-0112 requires. A reimport command cannot
forge an original actor, time, causation chain, or commit sequence.

## Options Considered

1. **Allow ordinary creates to set any workflow state:** rejected because a
   normal role could bypass the declared state graph.
2. **Reset every workflow to its initial state:** rejected because it silently
   changes authoritative application state and breaks reconciliation.
3. **Restore active leases exactly:** rejected because an old worker could
   retain apparently valid ownership across a database replacement.
4. **Use raw entity import under operator authority:** rejected because
   authority does not make an untyped storage bypass safe.
5. **Require every workflow adapter to hand-author a storage migration:**
   rejected because a migration cannot consume portable external records and
   would duplicate the same unsafe mechanism.
6. **Distinct compiled, create-only, quiescent workflow reconstitution:**
   proposed because it closes portability while keeping the unsafe operation
   unrepresentable from application code.

## Consequences

- MLflow and Woodpecker can cross an incompatible alpha format without
  weakening normal workflow commands.
- Operators must quiesce leased work and produce a new snapshot before
  reimport; portability-intent export proves this while the source remains
  available. This is deliberate downtime in a breaking-format ceremony.
- Relationship cycles and historical-event fidelity remain explicit
  unsupported cases unless a later accepted plan handles them.
- Installation state, command IR, capabilities, public protocol, driver-host
  schema, generated operator bindings, and receipts gain versioned successors.
- A partial reimport consumes storage but never becomes a ready application;
  cleanup is explicit and destructive, not automatic rollback.

## Compatibility

This is additive pre-alpha source syntax but changes contract grammar/IR,
application source/lock identities, capability encoding, installation campaign
state, service-operation registry, protobuf, and generated operator schemas.
Old binaries must refuse the new command/capability/campaign versions before
mutation. The implementation package owns one receipted repository-wide
rotation and retains old decoders and compatibility fixtures.

No existing durable entity, command, event, journal, changelog, or backup
record is reinterpreted. Reimport produces ordinary current-format command and
entity records in a new database; replication continues to derive from the
published destination frontier rather than from export bytes.

## Security

The reimport credential is operator authority scoped to one exact campaign and
manifest, never a bearer for general application writes. Server-selected
destination identity, not-ready gating, exact artifact hashes, complete-record
typing, current policy evaluation, compiler-derived ordering, and final
reconciliation prevent confused-deputy and partial-publication paths.
Diagnostics contain symbols, hashes, counts, and recovery actions but no row
values, secrets, credentials, filesystem paths, or hidden-schema facts.

## Standing Design Tests

- **Interface safety:** an application developer or agent cannot invoke,
  advertise, delegate into, or emulate workflow reconstitution through an
  ordinary command, generated application method, MCP tool, raw row writer, or
  caller-selected state/lease transformation. The only accepted shape is an
  exact create-only compiler plan inside a not-ready reimport campaign.
- **Scale:** pages, command batches, dependency schedules, checkpoints,
  diagnostics, and observations are bounded. Reimport streams records and
  never loads the export or destination database into memory; relationship
  cycles are rejected rather than solved by an unbounded fix-up pass.

## Testing

- Grammar/IR/generator fixtures and source-span negatives for ordinary state
  writes, mutable reconstitution, partial records, caller idempotency, active
  leases, hidden operation discovery, and dependency cycles.
- Deterministic command tests proving exact state/fence/attempt preservation,
  a first post-reimport claim strictly greater than a nonzero preserved fence,
  exhaustion without mutation, first destination revision, invariants,
  references, uniqueness, row-policy, provenance, and replayed outcomes.
- Portability-intent export tests proving canonical per-workflow quiescence
  evidence, refusal before a completed receipt for active and partially cleared
  leases, source-side recovery guidance, general-export non-upgradability, and
  reimport rejection of missing, mismatched, or forged quiescence evidence.
- Process crash schedules before/after each page command, checkpoint,
  reconciliation observation, receipt seal, readiness publication, and
  credential publication.
- Capability successor, old-binary refusal, delegation, revocation, expiry,
  cross-database/manifest substitution, secret-redaction, and application-role
  denial tests.
- Four-domain remote export-to-empty-reimport acceptance through Rust, Go,
  TypeScript, and Python operator surfaces for OpenFGA, MLflow, Better Auth,
  and Woodpecker; MLflow/Woodpecker include noninitial workflow states and
  quiescent nonzero fences. The retained Payload fixture remains a post-alpha
  portability regression.

## Requirements and Work Packages

- **Requirements:** `EXP-011` through `EXP-014`, `WF-001` through `WF-014`,
  `APE-001` through `APE-014`, and the existing security/safety requirements.
- **Defines or blocks:** WP-575, WP-578, WP-579, and WP-599.
- **Final evidence:** WP-599, WP-578, and WP-579.

## Decision Deadline

Exact human acceptance is required before WP-599 changes any grammar, IR,
capability, campaign, protocol, runtime, or durable interface.
