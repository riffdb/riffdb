# ADR-0056: Agent Application Alpha Before Operational and Distributed Alpha

- **Status:** Accepted
- **Proposed:** 2026-07-29
- **Direction approved:** 2026-07-29
- **Exact text accepted:** 2026-07-29
- **Acceptance reference:** Maintainer direction in the current Codex session
  to build the complete planned Agent Application Alpha phase
- **Requires:** ADR-0005, ADR-0006, ADR-0007, ADR-0008, ADR-0009,
  ADR-0013, ADR-0018, ADR-0027, ADR-0040, ADR-0041, ADR-0051,
  ADR-0052, ADR-0053, and ADR-0055
- **Amends if accepted:** SPEC Sections 20 and 24
- **Decision gate:** Satisfied by the acceptance reference above
- **Amended by:** ADR-0097

## Context

WP-205 through WP-300 turn RiffDB's kernel proof into a safe symbolic
application surface. TicketDesk demonstrates one named query per page,
one compiled command per mutation, one-snapshot execution, symbolic values,
generated operation artifacts, safe application authority, and a bounded local
workflow.

That evidence does not yet prove that a new application author can remain on
the product surface without reconstructing transport and authorization glue.
The TicketDesk implementation still exposes gaps that a fresh coding agent is
likely to fill with handwritten parameter maps, result decoders, RPC wrappers,
capability rituals, sequential seed loops, or direct kernel imports. Compiler
diagnostics retain useful context, while several public runtime errors collapse
to a category without the operation, symbol path, missing requirement, or
bounded remediation needed for self-correction.

Replication would multiply the compatibility cost of these application-facing
interfaces without resolving them. The next milestone should therefore be an
application-authoring alpha, distinct from the later single-node operational
alpha.

## Decision

### Gate placement

RiffDB adds an **Agent Application Alpha** gate after WP-300 and before
single-node operational hardening, replication, or partitioning.

The gate asks:

> Can a fresh coding agent build an unfamiliar application from an empty
> repository using only RiffDB's public textual, generated, CLI, and MCP
> surfaces?

The gate is not satisfied by TicketDesk alone, generated-code compilation, or
an in-repository example maintained by RiffDB implementers.

### Complete generated application bindings

Generated Rust and TypeScript clients own the complete stable-application
binding:

- typed parameter encoding;
- exact named-query and named-command invocation;
- typed query result and declared-outcome decoding;
- cursor encoding and decoding;
- exact contract and query-module identity negotiation;
- typed read-after-commit options;
- structured public application-error decoding;
- retry-safe command submission and outcome recovery; and
- MCP schemas for the same named operations.

Generated command helpers never invent a replacement idempotency identity on
retry. The exact caller-supplied or configured durable identity remains
observable and reusable for uncertainty recovery. Contract/module mismatch
fails closed; a generated client never silently negotiates down to a different
shape.

A first-party stable application contains no handwritten RiffDB parameter map,
record encoder, result decoder, outcome switch over numeric tags, RPC wrapper,
field mask, encoded key, or transport-status parser.

### Symbolic application manifest and roles

One canonical application manifest binds contract source, named query modules,
symbolic application roles, generation targets, and development seed inputs.
The exact manifest syntax is frozen by WP-305 and WP-310 fixtures before use.

A role names only application operations:

```riff
role TicketDeskAgent {
  query GetTicket
  query ListTickets
  query TicketPage

  command CreateTicket
  command CreateComment
  command AssignTicket
  command ChangeTicketStatus
}
```

The role compiler derives exact lineage/module/operation authority, contract
description access, private query field/index/partition/cost requirements,
command invocation authority, result visibility, and MCP tool visibility.
Those derived query requirements remain sealed inputs to the application-query
authorization proof from ADR-0055; they do not become reusable kernel
`ReadEntity` or `ScanIndex` grants.

Application-facing role binding names a role, principal, environment, and
tenant scope. It never accepts field IDs, field-mask order, index IDs, raw
capability masks, or an implicit `ReadContract` ritual.

### Structured application errors

The application API carries one bounded, versioned semantic error envelope.
When a failure is attributable to an application operation, it contains:

- stable code, category, and retry/recovery action;
- exact operation kind and name;
- authorized contract lineage/version and named query-module identity;
- a bounded authorized symbol path and caller-source span when applicable;
- a bounded symbolic required permission or resource;
- a static public message;
- zero or more closed, bounded suggested-fix codes; and
- a trace/incident identifier when server-side correlation exists.

Infrastructure failures that have no contract symbol still identify the
attempted operation, retry/recovery action, and trace identifier. They never
fabricate a field path.

The service performs authorization and redaction before attaching context.
Public envelopes contain no submitted free-form values, bearer material,
internal error sources, hidden schema, arbitrary server prose, or principal
secrets. CLI, Rust, TypeScript, and MCP render the same decoded semantic value;
logs may retain internal sources only under the existing incident boundary.

### Bounded command batches

Bulk seed and import remain command execution, not generic bulk storage.
A batch:

- selects one exact named command;
- processes a bounded stream with explicit concurrency and backpressure;
- gives every item its own canonical input and idempotency identity;
- returns every item’s typed declared outcome or structured public error;
- never presents the collection as one atomic transaction;
- supports cancellation, progress, resumable checkpoints, and same-key replay;
  and
- correlates item provenance with one bounded import/seed session identity.

The batch orchestrator calls the same application service, authorization,
command runtime, and commit coordinator as a unary invocation. It receives no
storage transaction, generic mutation, sequence allocation, or weakened audit
path. `riffdb dev --seed` uses this mechanism.

The exact choice between client-streamed orchestration and an additive
application RPC, its durable/session fields, and its resume receipt format
requires compatibility review in WP-320.

### Canonical project and package boundary

The normal entry point is:

```text
riffdb new <application>
cd <application>
riffdb dev
```

The generated repository makes the supported path visible through its
application manifest, contract, named queries, symbolic roles, seed inputs, and
generated Rust/TypeScript output roots. `riffdb dev` starts the local server,
compiles and deploys the contract/query module, binds the selected role,
generates clients and MCP schemas, runs the bounded seed, and watches accepted
source inputs.

Rust and TypeScript application facades exclude kernel requests by default.
Kernel/admin imports require an explicit unstable or administrative package/
feature and credential. Templates, quickstarts, generated clients, and normal
MCP catalogs never include that dependency. A mandatory boundary linter rejects
kernel imports and handwritten application transport glue.

This decision does not require the illustrative package names
`riffdb-app`, `riffdb-admin`, or `riffdb-kernel-protocol`; WP-325 must choose an
interface compatible with the existing crate graph and receive review before
adding or renaming a first-party package.

### Rust and TypeScript parity

Rust and TypeScript generation is driven by the same application manifest,
operation schemas, compatibility fixtures, and golden observations. Both
languages must complete the same application workload with equivalent:

- generated query and command methods;
- typed outcome unions;
- optional and nested results;
- pagination and cursors;
- read-after-commit;
- error decoding and recovery actions;
- development credentials and hot reload; and
- boundary-lint results.

The TypeScript proof is a working web application, not only a type-checking
fixture.

### Evidence-driven RiffQL growth

Two unfamiliar domains are attempted before adding another general query
construct:

1. a blog/CMS with public/private visibility, slug lookup, feeds, previews,
   comments, tags, moderation, and pagination; and
2. orders/inventory with headers and lines, product/customer lookup, inventory
   invariants, state transitions, bounded totals, history, dashboards, and
   batch import.

Every unsupported shape is preserved as a source-spanned fixture and classified
as unbounded, cross-partition, unindexed, unauthorized, unsupported cardinality,
or genuinely missing bounded expressiveness. Diagnostics identify the relevant
index, relationship, projection, or decomposition when one is sufficient.

No candidate such as `exists`, bounded aggregate, computed field, fragment,
named relation, search index, or materialized source is accepted by this ADR.
WP-335 must first publish repeated application evidence and stop for a separate
human-approved language/IR decision before implementing a new construct.
General SQL and arbitrary joins remain outside the application critical path.

### Independent evaluation

The final evaluation uses a sealed public product bundle containing only:

- `riffdb new` and `riffdb dev`;
- contract, RiffQL, role, generated-client, CLI, and MCP documentation;
- builder MCP;
- public generated Rust and TypeScript packages; and
- no RiffDB implementation source or TicketDesk source.

At least four fresh independent runs cover both domains in both Rust and
TypeScript. Each run records:

- human interventions;
- attempted and successful kernel use;
- handwritten RiffDB glue lines;
- compiler/runtime failures per completed feature;
- unsupported query shapes;
- time to first successful write;
- time to first page-shaped read;
- time to the complete workload;
- whether implementation source was inspected; and
- final rating and explanation.

Gate thresholds are:

- zero human product-workaround interventions after the brief;
- zero successful kernel operations and zero kernel imports in the result;
- zero handwritten RiffDB transport/encoding/authorization glue;
- no RiffDB implementation-source or TicketDesk-source access;
- first successful write within 30 minutes and first page-shaped read within
  60 minutes in every run;
- every required workload feature completed without an unresolved query shape;
  and
- every independent rating at least 8.5/10.

Infrastructure outages may be excluded only when the report identifies them
separately and a same-input rerun succeeds. A failed product gate produces an
issue against the owning work package; the evaluator does not patch around it.

## Compatibility

The compatible `riffdb.v1` kernel protocol remains supported and separately
privileged under ADR-0055. Application bindings, role manifests, error context,
and batch operations are additive versioned application surfaces.

Any new public Protobuf field/RPC, capability permission, manifest grammar,
generated signature, error code, batch receipt, or durable provenance field
requires exact old/current fixtures and the human review named by its work
package. Existing generated clients continue to fail closed on an incompatible
contract or module rather than receiving silent shape adaptation.

No replication, partitioning, SQL, generic bulk write, application transaction
callback, or storage bypass is introduced by this milestone.

## Consequences

- The next product gate optimizes for successful application authorship rather
  than distributed-system breadth.
- Code generation becomes a complete binding for stable applications while
  RiffQL remains the canonical query source.
- Role authoring becomes symbolic without weakening ADR-0055's private derived
  authority.
- Error payloads become more useful and more security-sensitive, requiring
  strict bounded vocabularies and redaction tests.
- Seed/import throughput improves without creating an alternate mutation
  semantics.
- RiffQL grows only from repeated application evidence.
- Replication is delayed until application-facing protocol and client shapes
  are credible enough to preserve.

## Rejected alternatives

1. **Proceed directly to replication.** Rejected because it freezes incomplete
   client, role, error, and batch surfaces into a distributed compatibility
   burden.
2. **Treat generated types as sufficient code generation.** Rejected because
   applications still rewrite transport and decoding glue.
3. **Expose derived field/index grants to application roles.** Rejected because
   they would recreate kernel-shaped authority outside the private query proof.
4. **Use generic bulk insert for seed/import.** Rejected because it bypasses
   command outcomes, idempotency, provenance, invariants, and authorization.
5. **Put arbitrary prose and full schema context in public errors.** Rejected
   because it leaks hidden structure and creates unbounded model-facing text.
6. **Add likely RiffQL features before new-domain evidence.** Rejected because
   breadth would weaken bounded planning without proving product need.
7. **Accept a TypeScript compilation fixture as parity.** Rejected because it
   does not exercise a real application runtime, errors, credentials, or
   development loop.

## Work-package mapping

- WP-305: complete generated application bindings and manifest.
- WP-310: symbolic role compiler and role binding.
- WP-315: bounded structured application errors across every public surface.
- WP-320: resumable command batches and seed/import workflow.
- WP-325: canonical scaffold, dev orchestrator, packaging, and boundary lint.
- WP-330: first-class TypeScript web-application parity.
- WP-335: two-domain evidence, diagnostic corpus, and reviewed RiffQL closure.
- WP-340: sealed independent Agent Application Alpha evaluation and release
  gate.
