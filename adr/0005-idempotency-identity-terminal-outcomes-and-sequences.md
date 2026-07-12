# ADR-0005: Idempotency Identity, Terminal Outcomes, and Sequence Semantics

- **Status:** Proposed
- **Direction approved:** 2026-07-12
- **Exact text accepted:** No
- **Decision deadline:** Identity before WP-060 durable keys; full record before WP-100

The human architecture review approved this direction. This record remains
Proposed until its exact text is reviewed and accepted.

## Context

Uncertain responses are recoverable only if a retry finds exactly the same
admission and terminal result without re-executing effects. The specification
uses both request IDs and caller idempotency keys but does not fully define their
scope, deployment behavior, or pre-terminal crash state.

## Proposed Decision

`RequestId` identifies an invocation for tracing; it is distinct from the
caller-provided idempotency key. Durable idempotency identity is:

```text
database/environment
+ authorization-resolved tenant scope
+ stable principal ID
+ contract lineage
+ stable command ID
+ keyed digest(caller idempotency key)
```

The contract version is stored with admission and outcome records but excluded
from lookup identity so a retry survives compatible deployment. Raw caller keys
are never persisted or logged. Canonical input is hashed with an explicitly
versioned, domain-separated algorithm.

Before runtime evaluation, the coordinator durably creates or resumes a bounded
pending admission containing the identity, canonical input hash, request ID,
contract version, plan hash, and fixed `tx.time`. A pending admission has no
commit sequence. The capability/lease is not durable. Resume reuses time and plan
and reacquires current non-durable capabilities.

At terminal commit, sequence, mutations, persisted `CommittedOutcome`, events,
provenance, and commit record become atomic. Same identity and input returns the
stored outcome and original sequence without execution. Different input returns
a safe mismatch error without execution. Declared terminal business rejections,
including zero-mutation rejections, receive exactly one sequence on first
terminal commit; replay receives no new sequence.

## Options Considered

1. **Scoped caller key with durable admission:** Approved uncertainty-recovery
   model.
2. **Request ID alone:** Conflates tracing and caller retry intent.
3. **Include contract version in lookup:** Breaks retries across deployment.
4. **Double-check without a durable pending record:** Cannot preserve admitted
   logical time and plan after pre-terminal crash.

## Consequences

- Admission records need explicit abandoned/resumable recovery and bounded
  retention policy.
- Compatible deployment cannot silently reinterpret an admitted command.
- Business rejection is data, not a transport failure.
- Read-only command sequencing, if any are admitted as commands, follows the same
  terminal-outcome rule unless a later accepted ADR narrows command semantics.

## Compatibility

Identity bytes, digest key/version, canonical input hashing, pending-record
version, terminal outcome encoding, and sequence semantics are durable boundaries.

## Security

Tenant scope comes from authorization, never caller claims. Keyed digests limit
offline disclosure of low-entropy caller keys. Safe mismatch errors reveal no
stored input or key. Digest keys require rotation/version and protected server
configuration.

## Testing

Golden identity and input-hash vectors; scope-separation properties; equal/mismatch
tests; deployment-between-retry tests; crashes after reservation, evaluation,
durable commit, and before response; concurrent same-key tests; and assertions for
one sequence, mutation, event set, provenance record, and outcome.

## Requirements and Work Packages

- **Requirements:** `TXN-010`, `TXN-040` through `TXN-044`, `ID-004`, `ID-005`,
  `REC-001` through `REC-003`, `LOG-001`
- **Defines or blocks:** `WP-010`, `WP-060`, `WP-070`, `WP-100`, `WP-130`
- **Final evidence:** `WP-190`, `WP-200`

## Decision Deadline

Accept identity, digest, and durable admission shape before WP-060/WP-070 freeze
keys. Accept all terminal and recovery semantics before WP-100 implementation.
