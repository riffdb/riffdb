# ADR-0005: Idempotency Identity, Terminal Outcomes, and Sequence Semantics

- **Status:** Accepted
- **Direction approved:** 2026-07-12
- **Exact text accepted:** 2026-07-12, amended 2026-07-12
- **Decision deadline:** Identity before WP-060 durable keys; full record before WP-100

The human maintainer accepted this exact text, including the identity-component
amendment, on 2026-07-12.

## Context

Uncertain responses are recoverable only if a retry finds exactly the same
admission and terminal result without re-executing effects. The specification
uses both request IDs and caller idempotency keys but does not fully define their
scope, deployment behavior, or pre-terminal crash state.

## Decision

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
are never persisted or logged.

The foundational component representations are fixed by ADR-0011. Database
identity is UUIDv7 network-order bytes. Environment and contract lineage are
their exact bounded text bytes. Tenant scope is a one-byte tag (`0` for global,
`1` for a tenant) and, for a tenant, the exact bounded tenant-ID bytes. Variable
text components are `u32` length-prefixed. Principal identity is the exact
bounded `ActorId`; command identity is `CommandId` as `u32` big endian. The
keyed caller digest contributes scheme byte, `DigestKeyId` as `u32` big endian,
and 32 digest bytes. WP-060 will place these components in a separately
versioned idempotency storage-key envelope; it may not reorder, omit, normalize,
or reinterpret them.

The v1 caller-key digest is HMAC-SHA-256 over the keyed hash frame defined by
ADR-0011, using domain `riffdb.idempotency-key/v1` and the exact validated UTF-8
bytes of the caller key as payload. The stored identity contains digest scheme
version `1`, a `DigestKeyId`, and the 32-byte digest. It does not contain the raw
key. A new admission uses the current write key. Lookup computes a bounded set of
candidate digests using the current key and explicitly configured readable
previous keys, newest first. Exactly one existing identity may match; multiple
matches fail closed as an integrity error. A key cannot be retired until all
outcomes that need same-key recovery have expired or been migrated under a
separately reviewed procedure.

The v1 canonical input hash is SHA-256 over the unkeyed hash frame defined by
ADR-0011, using domain `riffdb.command-input/v1`. Its payload is the
schema-validated canonical input record with the contract-declared idempotency-key
field omitted. The HMAC identity already binds that field; omitting it from the
unkeyed hash avoids creating a dictionary oracle for low-entropy caller keys.
Input field order, decimal scale, text bytes, and nested value encoding are
therefore independent of request serialization and map insertion order.

Before runtime evaluation, the coordinator durably creates or resumes a bounded
pending admission containing the identity, canonical input hash, request ID,
contract version, plan hash, and fixed `tx.time`. A pending admission has no
commit sequence. The capability/lease is not durable. Resume reuses time and plan
and reacquires current non-durable capabilities.

At terminal commit, sequence, mutations, terminal idempotency state and pending
resolution, persisted `CommittedOutcome`, events and outbox intent, provenance,
and commit record become atomic. Same identity and input returns the stored
outcome and original sequence without execution. Different input returns a safe
mismatch error without execution. Declared terminal business rejections,
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
- Read-only operations are journaled only when idempotency or audit policy
  requires it and otherwise create no mutation commit record, matching the POC
  default pending final WP-100 review.

## Compatibility

Identity component bytes and order, digest scheme and key version, canonical
input hashing, pending-record version, terminal outcome encoding, and sequence
semantics are durable boundaries. Adding a readable digest key is compatible.
Changing the HMAC algorithm, framing, identity tuple, component encoding, or
existing domain label requires a new version and migration plan.

## Security

Tenant scope comes from authorization, never caller claims. Keyed digests limit
offline disclosure of low-entropy caller keys. Safe mismatch errors reveal no
stored input or key. Digest keys require rotation/version, bounded previous-key
lookup, and protected server configuration. The Rust cryptography provider is a
separate critical-dependency review; it may not change these bytes.

## Testing

Golden identity and input-hash vectors; scope-separation properties; equal/mismatch
tests; deployment-between-retry tests; crashes after reservation, evaluation,
durable commit, and before response; concurrent same-key tests; and assertions for
one sequence, mutation, event set, provenance record, and outcome.

## Requirements and Work Packages

- **Requirements:** `TXN-010`, `TXN-040` through `TXN-044`, `ID-004`, `ID-005`,
  `REC-001` through `REC-003`, `LOG-001`, `OUT-001` through `OUT-004`
- **Defines or blocks:** `WP-010`, `WP-060`, `WP-070`, `WP-100`, `WP-130`
- **Final evidence:** `WP-190`, `WP-200`

## Decision Deadline

Accept identity, digest, and durable admission shape before WP-060/WP-070 freeze
keys. Accept all terminal and recovery semantics before WP-100 implementation.
