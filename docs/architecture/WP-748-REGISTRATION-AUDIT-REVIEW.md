# WP-748 — follower registration audit authority

Status: exact text accepted by the maintainer in session, 2026-09-16:
"Approve exact text". Authority-only acceptance commit: `05d2938b`.
Implementation and required proofs remain open.
Package: WP-748. Tier: guarantee. This does not close WP-748 or authorize a
promotion implementation or a new old-primary fencing mechanism.

## Contract gap

ADR-0178 section 6 requires audited follower registration/retirement and
configured-expiry release. SPEC section 13.5 requires shared-service audit with
safe stable targets and an exact authoritative control-plane result sequence.

ADR-0021 fixes the target registry to ten variants. None can identify the
source-lineage-scoped opaque follower hold selected by these operations. Its
Decision explicitly says: "Adding, removing, retagging, or changing the scope
or payload of a variant is a shared semantic and durable compatibility change
requiring a later accepted ADR." Database IDs are explicitly excluded from the
current independent-target registry. A follower ID is neither a capability ID
nor an event-consumer identity, and emitting an empty target list would omit the
object selected by registration/retirement.

The existing `StoredRetentionAdministrationV1` is also projection-only: its
closed actions are detach/reattach and its target is a projection ID. Reusing it
would reinterpret existing bytes. `ReplicationSourceHold` attribution 27 is
explicitly an empty physical receipt with no administration advance; adding an
audit mutation under that tag would contradict ADR-0186.

The checked hold V2 codec in `bf5a037b` activates no policy writer. Existing
source holds and retention fences remain effective while this gap is reviewed.

The storage-API regression test
`frozen_audit_v2_refuses_a_follower_target_even_with_a_valid_envelope_checksum`
inserts the proposed bounded target at field 11 into an otherwise valid V2
audit, preserving field order, existing targets and the target-count bound.
It recomputes the envelope checksum and verifies that the existing codec refuses
the unknown target. This refusal must remain after a successor is introduced.

## Exact accepted amendment

The following qualifies SPEC section 13.5, ADR-0021's target registry and durable
compatibility decision, and ADR-0178's unchanged-durable-record statement. It
implements registration/retirement audit in the existing authority namespaces.

> Add one closed semantic audit target, `ReplicationFollower`, at tag `0x0b`.
> Its payload is the request-selected source database ID, nonzero history
> incarnation, nonzero leadership epoch, and existing nonzero opaque 16-byte
> replication hold ID. It identifies one follower registration within one source
> lineage. It is not an authorizing capability or a result-derived target.
>
> Its canonical key is `0x0b || database_id:16_network_order_bytes ||
> history_incarnation:u64_be || leadership_epoch:u64_be || hold_id:16_bytes`.
> The existing zero-through-16 bound, duplicate refusal, canonical ordering,
> redacted diagnostics and shared-service ownership of target construction
> remain mandatory. Registration and retirement retain this exact target across
> started, denied, failed, uncertain and succeeded audit phases. No credential,
> address, business key, free-form reason or transport object is recorded.
>
> Add `ServiceAuditRecordV3` at durable record tag 22, revision 3, in a separate
> source file. Its target oneof preserves V2 fields 1 through 10 and adds
> `ReplicationFollower` as field 11. All other audit fields and lifecycle
> semantics retain their existing meaning. Preserve V1/V2 sources, descriptors,
> schema hashes, wire fixtures and readers; never reinterpret their bytes as
> containing the new target. New follower-target audits use V3; existing audits
> continue to use V2. Unknown or malformed target fields remain refused.
>
> Add `StoredReplicationAdministrationV1` at durable record tag 74, revision 1,
> in the existing administration-audit namespace. Its closed actions are
> `RegisterFollower`, `RetireFollower`, and `ExpireFollower`. One record binds
> its coordinator-assigned administration sequence and timestamp, the exact follower target,
> registration generation, bounded checked policy and before/after hold state,
> the initiating request/principal/approval for an explicit operation, and the
> original authorized registration receipt for configured expiry. The record
> contains no raw credential, path, address, business data or free-form reason.
>
> The coordinator-owned transition commits this record, its source-only hold
> change and the complete V3 receipt atomically at the drained writer barrier.
> Use the existing retention-administration attribution with its existing
> meaning; do not reinterpret attribution 27 or add another authority namespace.
> Exact replay returns the original authoritative administration sequence and
> cannot recreate a retired registration or release a different generation.
> Register/retire service success links that exact sequence through the existing
> `ControlPlane` result, preserving current authorization safe points and audit
> uncertainty rules. No caller may select a result sequence or waive audit.
>
> Configured expiry is an internal continuation of the exact previously
> authorized registration policy. It records `ExpireFollower` with that original
> receipt as provenance; it does not impersonate a new authenticated invocation.
> It may release only the matching registration after its configured sequence
> expiry and after typed replication health records degradation. Budget
> exhaustion alone never releases a fence. Startup, recovery and retention must
> validate and preserve required hold/control-receipt links and refuse missing,
> conflicting or substituted evidence. Retired registrations cannot acknowledge,
> bootstrap or resurrect under their old identity.
>
> Register both successor codecs through the existing registry/compatibility
> mechanism, with bounded canonical fixtures, mixed-generation reads, explicit
> old-binary refusal and the version topology. Prove atomic audit/hold changes,
> retry and crash recovery, current authorization, target equality, malformed
> record refusal and retention safety before enabling the public operations.
> These changes do not activate ordinary follower writes or local follower audit.

## Interface safety

Applications gain no unsafe option, raw row writer, audit override, arbitrary
callback or sequence allocator. The already accepted register/retire operations
remain administrative, capability-gated shared-service operations exposed only
through gRPC, the Rust operator client and CLI. A typed refusal is required when
authority, history, budget, receipt linkage or capacity cannot be proven.

## Scope and remaining work

This amendment does not decide promotion fencing evidence or relax REP-005.
WP-748 still owes the promotion operation, incarnation/epoch change, exact RPO,
health and all adapters. WP-749/750 qualification remains separate work.
Acceptance must be recorded in a standalone authority-only commit before the
new audit target or durable identities are implemented. Review artifacts and
regression evidence do not themselves mean the amendment is accepted.
