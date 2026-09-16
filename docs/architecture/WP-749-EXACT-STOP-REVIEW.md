# WP-749 — exact application stops inside physical groups

Status: accepted by the maintainer in session on 2026-09-15 (America/Chicago):
"Approve exact amendment". Standalone acceptance commit `d3088476` records the
exact text in SPEC §4.10 and ADR-0102/0178/0186. Implementation and proof remain
WP-749 work; acceptance does not expose exact-stop restore.

## Demonstrated conflict

REP-007 and the accepted archive receipt amendment in SPEC §4.10 require an
exact application-sequence stop, including inside a physical command group.
ADR-0186 §2 requires one net mutation per key in each physical receipt.
ADR-0102 keeps entity post-images outside command capsules; capsules retain
entity references, hashes, and logical transition evidence.

`tests/storage_recovery/v3_command_receipts.rs` now contains
`grouped_v3_receipt_retains_only_terminal_entity_image`. It uses the production
hardened command-group owner, two fully checked successful commands, and the
production published V3 receipt cursor. Both commands write the same entity.
The receipt advances application sequence 0 → 2 and contains exactly one entity
post-image: sequence 2's value. Sequence 1's commit remains readable, but its
entity reference contains a hash rather than the lost value. This is a supported
transaction-local serial group, not an invalid synthetic frame.

The test passes as evidence of current V3 behavior. It is not an exact-stop exit
proof. The existing source receipt cannot supply the missing intermediate value;
replaying its whole group overshoots the requested stop. A backup before the
group, final entity value, and hash of the intermediate value do not recover it.

A solution must retain additional information before it is lost. Silently
changing group formation, rounding the stop, relabelling partial data with the
source receipt hash, or weakening the required recovery granularity is outside
the accepted decisions.

## Exact accepted amendment

The following qualifies ADR-0102's command authority representation and
ADR-0178/0186's restore composition. It preserves complete V3 source receipt
semantics and REP-007's application-sequence recovery granularity.

> **Exact command-prefix restoration evidence.** Successful-command authority
> MUST retain sufficient bounded, canonical evidence to reconstruct the exact
> authoritative state after each application commit sequence inside a physical
> command group, including intermediate entity values overwritten or deleted
> later in that group. Hashes alone are insufficient. The evidence MUST cover
> the complete reciprocal command graph and all independently stored authority
> changed by that prefix; it MUST NOT depend on later live source reads,
> application callbacks, inverse hashes, or a surviving local journal.
>
> Introduce successor stored command capsule V7 (record tag 54, revision 6) and
> command segment V6 (record tag 55, revision 6) for this evidence. Evidence may
> reuse existing semantic mutation and command types. Its encoding MUST avoid
> self-reference, bind exact command order and original before/after frontiers,
> and validate against the complete original transaction's net mutations and
> canonical command facts. Startup and recovery MUST reject missing,
> contradictory, duplicated, noncanonical, or out-of-bound required evidence.
> The evidence is part of existing command authority in the existing namespace;
> it adds no authority namespace and changes no V3 frame or receipt encoding.
>
> The source MUST form and persist this evidence atomically with the existing
> command graph. Its bytes count against existing command, segment, transaction,
> journal and changelog ceilings, with capacity refusal before authoritative
> mutation. It adds no archive sink dependency, additional durability fence,
> acknowledgement gate, application control, or change to FIFO/grouping rules.
> WP-749 MUST provide same-workload write-path measurements and crash proofs.
>
> Exact archive restore MUST validate the complete selected original V3 frames
> and replay them through the existing follower applier. For a stop inside a
> physical transaction, the offline owner may reconstruct a separate private
> candidate from the validated predecessor and checked command-prefix evidence.
> That candidate is an offline restore artifact, never an attached follower,
> source receipt, acknowledged replication position, or serving database. It
> MUST NOT claim the full source transaction's identity, hash, or frontier.
>
> Before publication, the complete candidate MUST pass structural, catalog,
> reciprocal command, entity/index and exact-frontier validation, then staged
> authentication and current policy against that candidate. All readers MUST
> close before the durable receipt-authorized incarnation and RestoreAnchor
> publication ceremony. Receipt V3 retains the immutable original selection
> separately from the actual restored frontier. Interrupted private construction
> rebuilds from that selection; it never grants admission or publication authority.
>
> Existing stored capsule/segment bytes MUST NOT be reinterpreted or guessed
> into the new evidence. Register the successor identities and use the existing
> explicit compatibility/refusal and migration governance. Where old history
> lacks a recoverable intermediate value, refuse the requested exact stop before
> target replacement; never claim that old history gained sequence granularity.
> Such legacy refusal does not satisfy WP-749's exit gate for newly written
> supported history. Backup manifest V1, archive-manifest/v1, maintenance receipt
> V1/V2/V3, and changelog V3 bytes remain unchanged.

## Required proof before closure

Implementation status: the storage-api semantic validator retains each command's
independent mutations and checks logical frontier continuity, audit-frontier joins,
canonical order, cumulative bounds and exact reduction to the original receipt's
corresponding net mutations. Tests cover intermediate puts, deletes, recreations
and net-zero histories. Its bounded inner capsule payload codec shares the existing
V3 mutation encoding; byte-exact V3 fixture tests preserve that encoding. The inner
payload has no standalone record identity, checksum or publication authority.
Capsule V7/segment V6 envelopes and registry integration now require this evidence,
bind its application/audit frontier, reject mixed legacy/successor segments and
cover the payload in both normal and prepared segment seals. Generated vectors
freeze the successor bytes; prior schema hashes and V3 bytes remain unchanged.
The exact prior registry marker refuses before open; no automatic migration is
introduced. The follower applier now joins received successor groups to the
original receipt's independent net mutations and checks every first observation
against its pinned predecessor before applying that receipt, including keys
erased by net-zero reduction. Missing tables, contradictory values and mixed
legacy/successor command groups refuse without durable progress. Source staging
now captures each command's independent mutations and deferred logical index
epochs inside its existing private transaction, binding the exact audit-owner
predecessor. New audited commands write V7/V6 atomically. Pre-sequence sizing
charges physical chain heads and the complete nested prefix copy; its per-item
reservation floor also enforces the aggregate evidence-count ceiling through
the existing batch/epoch capacity check. A production same-entity group proves
put, replace, delete and recreate images and every intermediate index epoch on
standard journaled and hardened paths. Storage-owned startup/recovery decoding
and received successor groups now require exact entity, chain-head and index-epoch
mutation inventories and bind their typed post-images to each command's facts.
Pending-idempotency mutations may only delete the owning outcome identity.
Production corruption tests reseal missing, extra and substituted rows with
valid envelopes and prove refusal; valid grouped images still reopen. Received
entity, chain-head and epoch transitions now join to actual pinned predecessor
rows or borrowed earlier prefix images, with deletes shadowing physical rows.
Retained segments check their known intra-segment semantic predecessors and raw
mutation preconditions. Wrong nested record types cannot select a legacy decoder.
Real source/bootstrap/follower tests prove resealed false priors refuse before
any durable frame progress; valid put/delete/recreate groups replay and reopen.
Secondary-index puts now require a typed matching key, the command's schema
binding and its exact owning partition/index epoch. Available prior index images
also require that owning bucket before replacement or deletion. Resealed wrong
types and foreign partitions refuse in follower and retained startup tests.
Startup and follower replay use a pure catalog-owned validator to resolve
supplied index keys through the retained command bundle. Its shared IR encoders
derive each put's key and exact covered record from its entity post-image. A resealed undeclared cover
field refuses in production startup and follower tests. Direct bundle lookup
retains one bundle; mixed writer bindings additionally use the existing bounded
lineage proof for one resolved command plan. Supplied puts also require the
catalog-derived owning partition, and supplied deletions must belong to a command entity.
Follower validation now derives each entity's exact index mutation inventory
from its actual predecessor and post-image, rejecting missing puts/deletes and
extra mutations. Retained segments prove this for explicit creates and known
intra-segment predecessors; unknown historical live values remain unproven.
Resealed omissions preserve receipt/net agreement and refuse in both production
follower and retained-segment tests. A lazy catalog iterator now derives each
required prior index row, including unchanged indexes, and the backend compares
available actual rows against its key, cover and owning partition. Expected
covers are retained one at a time. Missing or malformed unchanged receiver rows
refuse before durable progress; unknown historical observations remain unproven.
Older writer images use the existing catalog materialization rules before
index derivation; raw writer bytes still own transition hashes. Historical schema
binding remains owned by the startup history proof or preceding validated prefix.
Intrinsic vector checks now bind typed evidence to its entity post-image,
command sequence, plan, provenance and partition. Each primary evidence mutation
requires exactly one reciprocal index mutation and its partition/lineage
observations, with command-owned revisions and nonempty persisted partition
counts. Re-sealed extra vector rows refuse in production startup and follower
tests. Missing links and inconsistent revisions, metadata and vector presence
also refuse; existing primary/follower vector traffic remains covered separately.
Catalog validation now rejects undeclared production fields and wrong rewritten
embedding metadata. Known entity predecessors determine source changes, vector
changes and required evidence mutation inventory. Available evidence and its
reciprocal index are joined to the actual predecessor; the shared checked
transition owner proves exact preserved source/embedding stamps. Unrelated
entity updates keep earlier evidence. Historical entity bytes use the same
bounded materialization proof as index derivation; unknown evidence remains
unknown. Exact predecessor counter arithmetic remains required.
This is not an exact-stop restore implementation. Complete secondary-index/vector
and cross-history graph validation, private reconstruction, publication proofs
and write-path measurements remain required.

- A real same-entity group with multiple puts, delete, and recreate restores to
  each application sequence with exact entity bytes and reciprocal command facts.
- Corrupt, missing or reordered prefix evidence refuses before replacement.
- Standard journaled and hardened groups, restart, checkpoint, archive retries,
  selected-prefix recovery and every publication crash boundary preserve the stop.
- Unchanged complete-group replication and legacy format refusal remain proven.
- Staged authorization reads the exact selected candidate; source and follower
  authority rules, bounds, redaction, and replacement confirmation remain intact.

## Review boundary

AGENTS.md requires human review when a required guarantee appears impossible
under an accepted ADR or a test reveals a SPEC/ADR conflict. D-003 also reserves
new durable identities and changes to accepted decisions for human review.
Approval of the prior archive receipt amendment did not authorize these new
command-authority encodings or private prefix-construction semantics. The
maintainer separately approved this exact amendment as recorded above.
