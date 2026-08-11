# ADR-0117: Compiled External-Framework Profiles

- **Status:** Proposed
- **Direction approved:** Not yet
- **Exact text accepted:** No
- **Decision deadline:** Before any framework-integration repository publishes
  an adapter claiming RiffDB support, and before WP-598 verifies the
  prerequisite surface

## Context

The alpha gate proves RiffDB can host real external applications, and the
maintainer has rescoped its adapter set to include Better Auth — an
authentication framework whose adapter contract is generic CRUD plus optional
transaction callbacks executed against a transaction adapter. That contract
shape is the exact anti-pattern this database's boundaries exclude: AGENTS.md
boundary 10 forbids arbitrary transaction callbacks and generic mutation
paths, and boundary 11 makes unsafety unexpressible at the public surface.
Sequential operations pretending to be atomic would be worse than either.

The same collision will recur for every framework whose persistence layer
assumes a generic store: the question deserves one durable answer, not a
per-integration improvisation. Two constraints bind the answer. First,
boundary 12: RiffDB does not implement end-user identity, sessions, sign-in
flows, or OAuth — an auth framework as flagship adapter is the proof that
applications keep their own auth while RiffDB is the data layer it runs on,
and that proof survives only if no framework logic crosses into the database.
Second, the maintainer's repository rule: outside documentation and workload
shapes, nothing specific to any external application belongs in first-party
crates; integrations live in dedicated repositories once the required
capabilities exist.

## Proposed Decision

A framework integration is a **compiled profile**: the framework's
configuration — including its selected plugins and their schemas — is input
to contract generation in the integration's own repository, producing a
normal RiffDB application (contract source, named compiled commands, roles,
policies) plus an adapter that implements the framework's persistence
interface by dispatching to those compiled commands.

1. **Generation time is the safety boundary.** Supported framework workflows
   map to named compiled commands; plugin schemas must be resolvable when the
   contract is generated; unsupported hooks, dynamic schema mutation, and
   transaction shapes that do not correspond to a compiled command fail at
   configuration/generation time with actionable diagnostics — never during a
   live authentication or data flow.
2. **The adapter is a dispatcher, not a store.** Generic CRUD entry points
   resolve `(model, operation, field set)` against the generated command
   surface and refuse unknown shapes. Framework "transactions" are supported
   exactly where a generated atomic command (including ADR-0107 bounded
   collection commands and ADR-0109 workflows) covers the callback's effect;
   otherwise generation fails closed. No adapter may batch sequential
   commands and report atomicity.
3. **External effects ride the outbox.** Verification email and equivalent
   side effects are durable outbox intents consumed by the application's own
   delivery machinery, never inline calls inside command execution.
4. **Idempotency is adapter-owned where the framework has no key.** The
   adapter mints and retains request identities for the framework's keyless
   retries using the drivers' existing client-identity machinery, so upstream
   retries land on RiffDB's normal exactly-once admission.
5. **Repository boundary.** First-party crates gain no framework-specific
   code, schema, naming, or dependency. This repository carries: the generic
   capabilities integrations need, the acceptance *workload shapes* in the
   gate corpus, and documentation. Each integration lives in its own
   repository, versions independently, and runs its framework's conformance
   suite (where one exists, e.g. Better Auth's adapter conformance suite) as
   an external acceptance instrument against a released RiffDB.

What RiffDB must therefore provide, as generic capability (verified or built
under WP-597/WP-598): declared unique constraints with transactional
enforcement; single-use/expiring token patterns expressible as compiled
commands (consume-once mutation plus invariants comparing stored timestamps
against transaction logical time); secret-field classification with
guaranteed display-surface redaction (ADR-0118); server-owned time and ID
service values; catalog surfaces sufficient for an external generator to
verify its produced contract against the deployed application; and TypeScript
driver distribution suitable for an npm-published adapter.

## Options Considered

1. **Implement the framework's generic adapter contract directly** (runtime
   CRUD, transaction callbacks): violates boundaries 10/11; rejected.
2. **Sequential emulation of transactions**: silent integrity loss under the
   framework's own documented expectations; rejected.
3. **In-repo integration crates**: couples release cadences, drags framework
   dependencies into the workspace, and erodes boundary 12's optics and
   substance; rejected by maintainer rule.
4. **Compiled profile in a dedicated repository** — accepted.

## Consequences

- Framework support becomes a generation problem with deploy-time refusal,
  matching the contract-first thesis; the supported-workflow set is explicit
  and versioned rather than emergent.
- Integrations lag framework plugin ecosystems by generator coverage; an
  unsupported plugin is a loud generation failure, not a degraded runtime.
- The gate's in-repo evidence is workload-shaped, not framework-named:
  acceptance fixtures model the semantics (unique identities, single-use
  tokens, session workflows) that any such framework forces.
- Each integration repository owns its release, conformance runs, and npm/
  package distribution; RiffDB's compatibility statement is its released
  driver surface.

## Compatibility

No public API, wire, or durable-format change in this record. Capability
gaps it names are delivered by their own ADRs/WPs (ADR-0118, WP-597,
WP-598).

## Security

The framework's secrets (session tokens, verification tokens, credential
hashes) are application data under RiffDB's normal authority model, with
ADR-0118 classification governing display surfaces. The adapter holds an
application principal's capability like any client; no integration receives
a privileged path. Generation-time refusal prevents the class of half-applied
authentication flows the framework's own documentation warns about.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** the profile adds no public
  surface; adapters consume the same named-command and query surfaces as any
  client, and no generation output can express an operation a hand-written
  application could not. The refusal of uncompiled transaction shapes is the
  mechanism that keeps the framework's generic contract from smuggling
  unsafety in.
- **Scale:** generation is per-deployment and offline; runtime dispatch adds
  a bounded lookup per call; nothing here assumes co-located storage or
  single-node memory.

## Testing

- In-repo: gate workload-shape fixtures covering unique-identity admission,
  single-use token consume/replay-refusal, atomic session workflows, and
  secret-field redaction (framework-agnostic, part of the alpha corpus).
- Per integration repository: the framework's own conformance suite against
  a released RiffDB, plus generation-refusal tests for unsupported plugins
  and transaction shapes.

## Requirements and Work Packages

- **Requirements:** capability gaps register under existing families plus
  ADR-0118's; this record adds none directly.
- **Defines or blocks:** WP-597, WP-598; blocks any integration repository's
  first release claim.

## Decision Deadline

Exact acceptance before WP-598's verification round concludes, and before
any integration repository is created.
