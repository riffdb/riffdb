# ADR-0075: Exact Successor Application Identity Before Mutation

- **Status:** Accepted
- **Date:** 2026-07-31
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `SAI-001` through `SAI-006`
- **Related work package:** `WP-399`
- **Amends:** ADR-0052, ADR-0057, ADR-0065, ADR-0066

## Context

The first multi-version dogfood application proved that compatible append-only
contract evolution preserves authoritative data, but exposed an identity split
between local application locking and server deployment. Local
`application lock --write` compiled every contract as genesis. The server
compiled the same successor source against the active parent. The source hash
therefore agreed while the canonical bundle and plan-root hashes correctly
differed.

Worse, `application deploy` called the mutating deployment RPC before comparing
the returned candidate with the lock. It could activate the server-compiled
successor and only then report that the lock disagreed. With no rollback, that
made a supposedly exact application deployment a one-way operation.

The same dogfood run showed that `application check` compiled sources only but
claimed that source, lock, and generated bindings were exact. The dedicated
`lock --check` correctly rejected the stale lock.

## Decision

### Safety rule

No application deployment may mutate catalog state until the server has proven
the exact candidate bundle hash from the application lock against the exact
active parent version and bundle hash. Candidate or parent mismatch is an
ordinary bounded, value-free result and performs no catalog admission,
deployment-state publication, query-module publication, role binding, or seed
command.

An already-active exact candidate is an idempotent success. It is not rejected
merely because the predecessor is no longer active. Any other active identity
is a mismatch, even when its numeric version agrees.

### Read-only successor compilation

The authorized contract-validation surface gains a read-only candidate-preview
mode. It snapshots the active contract, compiles source as its successor, and
returns:

- the exact parent version and bundle hash;
- the checked candidate descriptor and compatibility summary; and
- the bounded canonical candidate bundle bytes.

The operation performs no deployment or storage write. It requires existing
contract-authoring authority and uses the same compiler, catalog snapshot,
authorization, audit, redaction, and response bounds as deployment. Invalid
source and active-parent mismatch remain structured results.

### Application lock V3

Application lock V3 pins a compiler-owned canonical contract-bundle artifact at
`generated/riffdb.contract.bundle`. The lock stores the artifact's typed digest
alongside the existing contract, query, role, and generated-output identities.
The candidate bundle itself remains bounded by both the contract-IR and public
response limits and is strictly decoded and revalidated before use.

Genesis locking remains local and deterministic. Successor `lock --write` may
perform the accepted read-only candidate preview because the active parent is
part of successor identity. It never deploys. After publication, `lock --check`
and `generate --locked` use the pinned bundle artifact and remain offline.
Legacy V1/V2 genesis locks remain readable byte-for-byte. A legacy lock cannot
claim an exact successor identity and must be explicitly rewritten.

### Truthful application check

`application check` remains read-only. When the default lock exists, it checks
the source, exact lock, pinned contract bundle, and every generated artifact and
returns `RDB-AL008` on drift. When no lock exists, it reports only that symbolic
sources compile and explicitly states that no lock or generated artifact was
checked. It never emits the exact-lock success message for a source-only check.

### Deployment journal recovery

Partial deployment state remains resumable and non-authorizing. A changed lock
may reset incomplete contract/module/seed progress only after remote identities
are reverified. Retained role authority still requires explicit credential
replacement. Manual deletion of deployment state is never the normal recovery
path.

### Locked role reconciliation

Every role operation reached through an application lock consumes the pinned
canonical contract bundle and exact query modules from that lock. Recompiling
the contract source as genesis is not a valid role-resolution fallback for a
successor. Standalone role operations discover and verify the same lock when
they are given either the symbolic application source or its generated exact
manifest.

Application deployment compiles every requested role and materializes every
local query-module input before the contract deployment RPC. A local authoring
failure therefore performs no remote mutation. Widening a role changes its
definition identity and still requires explicit revocation and replacement of
retained authority; reconciliation is never implicit authority expansion.

## Compatibility

The validation and deployment messages receive additive pre-alpha fields and
result variants. Application lock V3 and its contract-bundle artifact are new
versioned local compiler outputs; V1/V2 decoding is unchanged. No authoritative
entity, event, commit, idempotency, provenance, catalog key, or storage envelope
format changes.

## Security

Canonical bundle bytes are returned only by the authorized read-only
contract-authoring operation, never by application roles or general contract
description. They are bounded, compiler-owned bytes and contain no credentials
or application values. Deployment compares exact parent and candidate hashes
before mutation and rechecks through catalog compare-and-swap. Transports do
not compile or reinterpret candidates.

## Testing

- Genesis and successor source with identical text hashing inputs prove that
  only parent-aware compilation produces the deployable identity.
- Candidate and parent mismatch tests assert zero catalog submissions and zero
  deployment-journal changes.
- A race replacing the active parent after preview fails closed before
  activation.
- Exact already-active retry returns success without a second deployment.
- V1/V2 lock fixtures remain byte-exact; V3 artifact substitution, truncation,
  path drift, and source drift reject.
- `application check` distinguishes source-only success from exact-lock success
  and reports `RDB-AL008` for stale source, lock, bundle, or bindings.
