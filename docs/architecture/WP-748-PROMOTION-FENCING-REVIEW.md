# WP-748 — durable primary fencing and audited promotion

Status: proposed; exact maintainer acceptance required before implementation.
Package: WP-748. Tier: guarantee. This is separate from the pending
`WP-748-REGISTRATION-AUDIT-REVIEW.md`; neither proposal is accepted by this file.

## Contract gap

REP-005 and ADR-0178 section 5 require proof that the old primary is fenced,
refusal of its command admissions, one audited promotion, a new incarnation
and leadership epoch, and an exact sequence-delta RPO.

The current `LeadershipEpochV1` is only a nonzero lineage counter. Its checked
successor does not revoke source authority. Revocation of `ReplicateChangelog`
removes a caller's stream permission; it does not revoke the primary's command
admission. No durable primary-admission state or primary-fence operation exists.
A stopped process, a failed connection, an operator boolean, a follower's lag
observation, and a checksum supplied by a caller cannot prove a persistent fence.

ADR-0186 section 5 permits **exactly five** additional metadata domains and the
bounded source-holds table. Its closed catalog currently ends at namespace 207.
The existing hold kinds describe retention custody, not source leadership.
Neither a spare epoch bit nor a fabricated hold may carry a primary fence.
Adding another metadata domain and its registry identity requires acceptance
under AGENTS.md and D-003. Promotion also needs a durable attempt record while
the attached follower still cannot originate database audit writes.

## Exact proposed amendment

This qualifies ADR-0178 sections 2 and 5, ADR-0186 sections 1 and 5 and its
namespace compatibility rule,
ADR-0019/STO-012's retained-metadata inventory, and SPEC section 13.5. It
implements the existing promotion requirement without automatic election.

> **Durable source admission fence.** Add a versioned
> `ReplicationPrimaryAdmissionV1` record in one new retained metadata domain,
> `replication_primary_admission/v1`. Classify it as required
> `ReplicationControl(SourceOnly)` in successor `AuthoritativeStateCatalogV2`,
> mandatory for source-mode opens. Attached followers do not carry this
> source-only admission state. Its closed states are Active and Fenced. Both bind the exact source database
> ID, history incarnation and leadership epoch. Fenced additionally binds one
> operation ID, selected follower registration/generation, immutable final
> application sequence and the exact authoritative fence-administration receipt.
> Missing, malformed or contradictory required state fails closed at startup.
> Old catalog and record identities retain their bytes and meaning.
>
> Add the administrative `FenceReplicationPrimary` operation and permission,
> exposed through the shared service, gRPC, Rust operator client and CLI only.
> It is never application-role bindable or an MCP tool. Fresh authorization and
> any required approval precede mutation. It stops new command admission,
> drains already admitted work through the existing coordinator barrier, and
> atomically persists Fenced and a `StoredPrimaryFenceAdministrationV1` record
> with its complete V3 receipt. The application head is unchanged by fencing;
> its observed value becomes immutable fence evidence. Register the closed
> `PrimaryFence` attribution; existing attribution meanings remain unchanged.
>
> Current and subsequent source opens MUST consult the checked admission state
> before granting a command writer or application readiness. A fenced source
> returns the typed `primary_fenced` RDB-REP outcome on command admission.
> Control operations that can grant new application authority, replace the
> database, or undo the fence are refused. Bounded authenticated fence retries,
> health/statistics, existing replication drain/acknowledgement and required
> service-audit writes may continue; none can advance the application head or
> clear the fence. No unfence operation is introduced. Physical copies do not
> lose the fence, and deleting or losing required metadata is corruption.
>
> Exact retries of fencing retain the original operation, follower generation,
> final application head and administration sequence. A conflicting operation
> cannot retarget the fence. Losing the response never restores write admission.
> The fence operation may succeed even if the later promotion cannot finish;
> that availability loss must be explicit in operator documentation.
>
> **Proof before promotion.** The promotion owner obtains and validates the
> fence through the configured source's existing authenticated TLS peer path,
> under current administrative authority. It never accepts a caller's claim
> that a source stopped or a plain serialized receipt as proof. The source must
> validate that the candidate's exact applied V3 position belongs to the fenced
> source's retained history. Foreign lineage, wrong registration/generation,
> unproven ancestry, a candidate beyond the fence, or unavailable first-time
> proof refuses promotion. Ordinary replication capability revocation is not
> such proof. No new cryptography or external election/fencing service is added.
>
> After source fencing, stop receiving new input and drain the follower's
> already received complete prefix through the sole applier's durability
> boundary. Bind the resulting exact position to the validated source fence.
> It need not catch up through unreceived source history. Compute RPO by checked
> subtraction of the applied application sequence from the frozen source
> application sequence, treating BeforeFirst as zero. Administration or physical
> transaction counts are never substituted for application sequences.
>
> **Audited offline cutover.** Add bounded external
> `replication_promotion_receipt/v1` under the existing exclusive maintenance
> owner. It is a distinct domain, not a restore receipt or replicated history.
> It retains the exact request, authenticated actor/capability/approval identity,
> validated fence, drained applied position, RPO, chosen incarnation/epoch and
> phase. It contains no bearer, raw transport object or free-form payload.
> Receipt retries cannot change selection, target, arithmetic or chosen values.
>
> Authenticated promotion attempts are durably audited in this external ledger
> while the node is an attached follower or offline promotion candidate. This
> is a promotion-only exception to local database service-audit placement in
> SPEC section 13.5; it does not authorize ordinary follower audit writers.
> Explicit denials and failed or uncertain attempts retain their bounded audit
> outcome there and release no application authority. Successful cutover binds
> the complete attempt and terminal result to a new-lineage
> `StoredPromotionAdministrationV1` and normal service-audit records atomically.
> The existing registration-audit proposal's follower target and successor audit
> codec must be separately accepted before use; this proposal does not accept it.
>
> With the receiver/applier drained and all readers closed, the exclusive owner
> retains the exact applied authority and uses the ADR-0072 stamp path with
> checked successors of the old incarnation and leadership epoch. The caller
> cannot select either successor.
> It starts the existing Promotion-attributed V3 anchor, records the promotion
> audit/control result and installs Active admission state for the new lineage
> in one validated cutover. Counter exhaustion refuses before mutation. Only
> complete ordinary source validation and matching receipt reconciliation permit
> primary readiness. Old tokens, cursors, frames, streams and acknowledgements
> fail under their existing lineage checks.
>
> Before durable cutover, restart requires fresh authorization for the exact
> operation and validated immutable selection; it never promotes automatically.
> After durable cutover, recovery may finish validation and terminal receipt
> publication only from exact committed authority and receipt evidence. It never
> chooses another frontier, increments the counters again or rejoins the old
> source. A missing or contradictory receipt fences readiness. External attempt
> evidence remains available for audit and is not silently discarded on failure.
>
> Register these named successor identities, bounds and compatibility fixtures
> through the existing registry and topology. This one SourceOnly domain is
> permitted without a successor changelog-frame identity: it adds no
> replicated-authoritative mutation namespace and cannot appear in V3 mutations.
> Preserve existing record, changelog V3 frame and backup-manifest V1 bytes.
> A new catalog binding does not
> relabel old receipts. Incompatible old registry/catalog markers refuse before
> open or target replacement; no automatic migration or inferred admission state
> is authorized. Any explicit upgrade must preserve the original history and
> fence semantics and receive its normal migration proofs before activation.

## Required proof before activation or closure

- Real source fencing under load: all admitted outcomes resolve, later commands
  refuse, and the fence survives repeated source crash/reopen.
- Source audit, receipt and admission state are atomic; uncertain retries retain
  one original fence and cannot select another follower or generation.
- Permission/approval denial, foreign or substituted proof, missing history,
  stale generation and unavailable proof all refuse without promotion.
- Crash each external receipt, drain, stamp, anchor, audit, validation and
  publication edge; recovery reaches one exact result or remains refused.
- `promotion_mints_incarnation_and_refuses_old_lineage_tokens` covers old tokens,
  cursors, frames, acknowledgements and streams, followed by an ordinary new
  command/read round-trip with provenance and idempotent retry.
- Prove zero and nonzero RPO from measured source/application frontiers in both
  storage profiles; refuse reversed frontiers and counter exhaustion.
- Preserve strict registration retention fences, old durable fixtures and
  bounded fail-closed startup; update operator and compatibility handbook pages.
- Complete WP-748 acceptance and WP-750's existing repeated-kill campaign.

## Interface safety and acceptance boundary

Applications gain no operation, unsafe option, fence bypass, raw writer or
sequence selector. A partition can prevent promotion; it cannot authorize it.
The implementation remains first-party Rust and introduces no quorum, automatic
failover, generic callback, new critical dependency or source hot-path durability
fence. Exact acceptance must be recorded in its own authority-only commit before
any of these new domains, operations, permissions or audit exceptions activate.
