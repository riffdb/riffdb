# ADR-0005: Idempotency Identity, Terminal Outcomes, and Sequence Semantics

- **Status:** Accepted
- **Direction approved:** 2026-07-12
- **Exact text accepted:** 2026-07-12, amended 2026-07-12, 2026-07-13, and 2026-07-14
- **Amended by:** ADR-0004 (complete executable-plan reference and semantic
  storage boundary), ADR-0007 (unjournaled read-only service result), ADR-0009
  (typed digest-key custody), and ADR-0012 (terminal non-commit execution failure)
- **Decision deadline:** Identity before WP-060 durable keys; full record before WP-100

The human maintainer accepted this exact text, including the identity-component
amendment, on 2026-07-12. The human maintainer accepted the exact storage-key
envelope/bound and companion amendments on 2026-07-13; they supersede only the
narrower points identified and do not change the durable identity tuple or
committed-outcome replay semantics.
The human maintainer accepted the exact single-terminal-row clarification below
on 2026-07-14.

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
and 32 digest bytes.

The complete v1 idempotency storage key is exactly:

```text
0x59 0x01
+ DatabaseId as 16 network-order bytes
+ u32_be environment byte length + exact environment bytes
+ tenant tag: global 0x00 or tenant 0x01
+ when tenant: u32_be tenant-ID byte length + exact tenant-ID bytes
+ u32_be ActorId byte length + exact ActorId bytes
+ u32_be contract-lineage byte length + exact lineage bytes
+ CommandId as u32 big endian
+ digest scheme byte, exactly 0x01 for v1
+ DigestKeyId as u32 big endian
+ exactly 32 caller-key digest bytes
```

The purpose byte is ASCII `Y`; the second byte is key format version 1. Using
the accepted maxima of 64 environment bytes, 256 tenant-ID bytes, 256 `ActorId`
bytes, and 256 lineage bytes, the largest complete v1 key is exactly 908 bytes:

```text
2 + 16 + (4 + 64) + 1 + (4 + 256) + (4 + 256)
  + (4 + 256) + 4 + 1 + 4 + 32 = 908
```

The global form omits the tenant length and bytes. A checked builder validates
every component and the complete ADR-0011 4,096-byte durable-key limit before
allocating the output. Reconstruction validates the purpose/version, every
fixed width, tag, declared text bound and UTF-8 rule, supported digest scheme,
nonzero `DigestKeyId`, full input consumption, and the complete-key
limit before returning a typed key. WP-060 may not reorder, omit, normalize, or
reinterpret any component, accept an unknown tag or scheme, or allocate from an
untrusted declared length before validating the complete bound.

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

At terminal commit, sequence, mutations, one full `StoredOutcome`, deletion of
the pending row, events and outbox intent, provenance, and commit record become
atomic. That `StoredOutcome` is both the terminal idempotency state and the
persisted outcome; it is not a pointer to a second record. Pending deletion
writes no tombstone envelope, and no second terminal or outcome envelope is
written. Same identity and input returns the stored outcome and original
sequence without execution. Different input returns a safe mismatch error
without execution. Declared terminal business rejections, including
zero-mutation rejections, receive exactly one sequence on first terminal commit;
replay receives no new sequence.

### 2026-07-13 companion amendments

Every pending admission and every snapshot, intent, stored outcome, commit, and
provenance record that identifies executable command semantics uses ADR-0004's
complete `ExecutablePlanRef`: contract lineage, contract version,
`ContractBundleHash`, stable command ID, and command `PlanHash`. The pending
record's earlier `contract version, plan hash` wording is therefore shorthand
for this five-field checked historical reference. The active plan is never
substituted during resume.

ADR-0009 fixes POC idempotency digest-key custody. `riffdb-auth` owns the
operational-secret provider and exposes only an idempotency-specific typed digest
capability to `riffdb-idempotency`; raw key bytes never enter that crate. The
provider document uses ADR-0009's exact one-to-eight-key entry grammar and exact
header `riffdb-idempotency-digest-keys-v1<LF>`, is distinct from the capability
token key namespace, and is cross-checked to reject reused key material.

Before readiness, the server scans every durable pending and terminal
idempotency identity: `Pending`, `StoredOutcome`, and `ExecutionFailed`. Each
identity's digest scheme must be supported and its `DigestKeyId` must exist in
the configured readable idempotency-key provider. The POC has no identity expiry
or migration procedure, so a readable digest key cannot be retired while any of
those records references it. A missing scheme or key fails readiness rather than
making an admitted command or terminal result unrecoverable.

The storage-owned durable result DTO is `StoredOutcome`: it contains the original
declared outcome, application sequence, and immutable commit context. The
transport-neutral WP-100 response wrapper is `CommittedOutcome`: it contains the
stored result plus current-invocation metadata such as `replayed`. Replay never
rewrites `StoredOutcome`; the older phrase "persisted `CommittedOutcome`" is
amended to this distinction.

### 2026-07-14 single terminal-row clarification

For a committed declared outcome, the canonical `idempotency` table value is
exactly one full `StoredOutcomeV1` `StoredEnvelope` keyed by the complete
idempotency identity. The same envelope is the terminal command-idempotency
state and the durable result returned by uncertainty recovery. There is no
terminal pointer, separate outcome table, second terminal envelope, or pending
tombstone. The atomic command transaction deletes the matching
`StoredPendingAdmissionV1` row while installing the outcome and its reciprocal
commit/provenance/event/outbox record graph.

On every startup, each stored outcome key must match its payload identity, each
outcome must identify exactly one commit at the same sequence, and every commit
must identify exactly one stored outcome. The two records must agree on every
shared immutable field, including admission request, executable plan, canonical
input hash, actor, logical time, partition/conflict hashes, declared outcome,
provenance ID, sequence, and durability mode; linked provenance carries the same
idempotency identity. A missing, duplicate, mismatched, or still-pending
reciprocal row is corruption and is never repaired.

ADR-0012 adds the terminal admission state
`ExecutionFailed { ArithmeticFault | ResourceLimit }`. It may be written only
after every influential entity absence/version and range epoch is equal in a
short transaction. It retains the pending identity, input hash, complete plan
reference, admitted actor/time/partition, and approved provenance-claim snapshot,
but creates no declared `StoredOutcome`, application `CommitSequence`, event,
outbox intent, application commit, or command provenance. It consumes the
idempotency identity; equal-input replay returns the stored failure, different
input remains `IdempotencyKeyReuse`, proven abort leaves the admission pending,
and unknown commit status is resolved through same-key recovery.

Grammar-v1 read-only commands are unjournaled under ADR-0004/ADR-0007/ADR-0012.
They create no pending or terminal command-idempotency record, persisted outcome,
provenance, or application sequence. Their required service audit is a separate
outcome-free administration record. This closed rule supersedes the earlier
consequence that left journaling conditional on later policy review; durable
read-only replay requires a future accepted ADR.

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
- Grammar-v1 read-only commands are unjournaled and create no command admission,
  persisted outcome, command provenance, or application sequence. Their required
  service audit remains a separate outcome-free administration record; changing
  this rule requires a future accepted ADR.

## Compatibility

Identity component bytes and order, digest scheme and key version, canonical
input hashing, idempotency key prefix/version and encoding, pending-record
version, the single full-outcome terminal-row shape, outcome/commit reciprocity,
and sequence semantics are durable
boundaries. Adding a readable digest key is compatible. Changing the HMAC
algorithm, framing, identity tuple, key component encoding/order, prefix,
version, or existing domain label requires a new version and migration plan.

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
one sequence, mutation, event set, provenance record, and outcome. Fixtures and
startup tests must prove atomic pending deletion without a tombstone, absence of
a second terminal/outcome envelope, exact outcome/commit/provenance reciprocity,
and rejection of every missing, duplicate, mismatched, or still-pending edge.
Startup tests
must cover every pending and terminal identity state, reject an unsupported digest
scheme or absent readable `DigestKeyId`, and prove that key retirement remains
blocked while any `Pending`, `StoredOutcome`, or `ExecutionFailed` record refers
to it. WP-060 key fixtures cover the exact global and maximum tenant forms,
the 908-byte maximum, malformed tags/schemes/lengths/UTF-8, trailing bytes, and
pre-allocation rejection of a declared complete key above 4,096 bytes. The
central ADR-0011 purpose-prefix fixture proves `0x59 0x01` has no collision.

## Requirements and Work Packages

- **Requirements:** `TXN-010`, `TXN-040` through `TXN-044`, `ID-004`, `ID-005`,
  `REC-001` through `REC-003`, `LOG-001`, `OUT-001` through `OUT-004`
- **Defines or blocks:** `WP-010`, `WP-060`, formal durable-schema `WP-065`,
  `WP-070`, `WP-100`, `WP-130`
- **Final evidence:** `WP-190`, `WP-200`

## Decision Deadline

Accept identity, digest, and durable admission shape before WP-060/WP-070 freeze
keys. Accept all terminal and recovery semantics before WP-100 implementation.
