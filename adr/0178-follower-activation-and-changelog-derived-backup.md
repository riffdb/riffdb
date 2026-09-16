---
adr: 0178
title: Follower Activation and Changelog-Derived Incremental Backup
status: accepted
tier: guarantee
date: "2026-09-01"
accepted: "2026-09-01"
acceptance: >-
  Original exact text accepted by the maintainer in the Claude Code session of
  2026-09-01; follower audit amendment accepted in session 2026-09-15, "I approve
  of these changes"; exact follower columnar amendment accepted in session
  2026-09-15, "Approve the exact amendment", referring to
  docs/architecture/WP-747-FOLLOWER-COLUMNAR-REVIEW.md; exact archive restore
  receipt amendment accepted by the maintainer in session 2026-09-15,
  "Approve exact text", referring to
  docs/architecture/WP-749-ARCHIVE-RESTORE-RECEIPT-REVIEW.md; exact command-prefix
  evidence amendment accepted in session 2026-09-15, "Approve exact amendment",
  referring to docs/architecture/WP-749-EXACT-STOP-REVIEW.md.
requires: [ADR-0019, ADR-0050, ADR-0061, ADR-0072, ADR-0082, ADR-0083, ADR-0085,
  ADR-0086, ADR-0093, ADR-0100, ADR-0101, ADR-0104, ADR-0112, ADR-0124]
# ADR-0085 is required at Amendments 2 and 3; ADR-0100 at Amendments 1 and 2.
amends:
  - SPEC 20.6 Stage B (the asynchronous changelog-shipping tier precedes and does
    not require OpenRaft or any consensus protocol)
  - SPEC 18.5 (incremental and remote archive backup move from MVP to alpha)
  - SPEC 13.5 (follower service and audit exceptions accepted 2026-09-15)
  - SPEC PRJ-005 through PRJ-009, ADR-0192 decisions 14–16, ADR-0195 decisions 4–6,
    and ADR-0209 decision 7 (exact follower columnar exception accepted 2026-09-15)
  - ADR-0050 restore input, RPC inventory and V1-only writer rules (exact archive restore receipt amendment accepted 2026-09-15)
  - REP-001 is unchanged
requirements: [REP-002, REP-003, REP-004, REP-005, REP-006, REP-007, REP-008, REP-009]
packages: [WP-746, WP-747, WP-748, WP-749, WP-750]
obligations:
  - id: OBL-0178-1
    package: WP-746
    proof: follower_applies_exact_prefix_byte_faithfully
    says: A follower driven by the app-baseline workload holds a byte-faithful
      prefix of every authoritative table at the compared applied sequence.
  - id: OBL-0178-2
    package: WP-746
    proof: replication_stream_resumes_gap_free_after_repeated_kills
    says: The stream resumes from the acknowledged sequence after repeated
      mid-flight kills with no gap and no duplicate apply, and the follower passes
      complete startup validation afterward.
  - id: OBL-0178-3
    package: WP-747
    proof: follower_causal_read_waits_for_token_then_matches_primary
    says: A Causal read on a follower waits for a fresh primary token and then
      returns bytes equal to the primary at the same frontier, while every write
      surface returns the typed follower-mode refusal.
  - id: OBL-0178-4
    package: WP-748
    proof: promotion_mints_incarnation_and_refuses_old_lineage_tokens
    says: Promotion mints a new history incarnation and leadership epoch, refuses
      every old-lineage token, cursor, and stream, and reports the sequence-delta
      RPO.
  - id: OBL-0178-5
    package: WP-748
    proof: retention_prune_refuses_to_pass_registered_follower_frontier
    says: Offline prune refuses to pass a registered follower's acknowledged
      frontier until it catches up or its hold budget is released by ceremony.
  - id: OBL-0178-6
    package: WP-749
    proof: archived_suffix_restore_recovers_to_last_archived_sequence
    says: Restore from a verified full backup plus the archived frame suffix
      recovers exactly through the last archived sequence and fails closed on any
      gap, checksum, or incarnation mismatch.
  - id: OBL-0178-7
    package: WP-750
    proof: replication_acceptance_campaign_meets_every_adr_0093_criterion
    says: The complete ADR-0093 acceptance campaign is a receipted release
      artifact.
review_triggers:
  - Acknowledgement semantics, the commit hot path, or the single-writer property
    on either node would change.
  - Automatic election, cascading followers, multi-primary, or a quorum tier would
    be introduced.
  - A new provider family, result-set algebra, or performance package would be
    admitted to the alpha gate before WP-750 closes.
  - A durable record encoding, the backup manifest V1, changelog frame bytes, or a
    retention tombstone byte would change.
---
# ADR-0178: Follower Activation and Changelog-Derived Incremental Backup

## Context

ADR-0093 was accepted on 2026-08-04 and states the problem in its own words: availability is "the biggest honest gap in the architecture review." A lost disk is a restore-from-backup event with data loss up to the last offline backup, and a crashed host is an outage for as long as the host is down. The product thesis in `docs/VISION.md` is a system of record for multi-tenant SaaS. A single node with offline-only backups cannot carry that thesis to a design partner, whatever else the engine does well.

Since that acceptance, ADR-0094 through ADR-0177 landed. They are dominated by query providers, result-set algebra, and performance packages. The replication surface itself advanced only as far as ADR-0100: the storage API carries `ChangelogFrameV1`/`V2`, stream validators, the `PublishedDurableSnapshot` read capability, `ChangelogPublicationPort`, and the redb emitter thread that derives frames from published snapshots without touching writer-private state. The V2 rotation receipt and the `DeleteAwareEntityFollowerV2` conformance oracle exist. Production still installs `NoChangelogPublicationPort` (`crates/riffdb-storage-redb/src/store.rs`), there is no replication RPC in `proto/riffdb/v1/services.proto`, no replication capability kind, no follower mode of `riffdbd`, no applier over the authoritative tables, no promotion operation, no follower retention fence, and no incremental backup. The known-limitations page still says "no replication, failover, or consensus."

Three facts make activation tractable without new design. There is one total order (ADR-0082) and commit records carry no post-images (ADR-0083), so an exact prefix is a valid database. Frontiers and commit tokens are already incarnation-bound and fail closed (ADR-0072, ADR-0086). Acknowledgement is already strictly after local durability (ADR-0061), so the asynchronous tier adds nothing to the commit hot path. ADR-0100 already aligned frames to published durable frontiers, which is exactly the boundary a follower may trust.

Incremental backup falls out of the same stream. A consumer that persists checksummed frames to a configured sink beside periodic full backups gives restore-to-last-archived-sequence without a second durable format, which the limitations page today lists as absent.

## Decision

### Accepted amendment: exact command-prefix restoration evidence

The exact "Exact command-prefix restoration evidence" amendment in SPEC §4.10
is incorporated here in full. The maintainer accepted it in session on
2026-09-15: "Approve exact amendment", referring to
`docs/architecture/WP-749-EXACT-STOP-REVIEW.md`. WP-749 owns successor capsule V7
(tag 54, revision 6), segment V6 (tag 55, revision 6), complete bounded prefix
evidence and validated private reconstruction before the unchanged receipt,
authorization and incarnation publication ceremony. Original V3 receipt semantics
and bytes remain unchanged; missing legacy evidence never permits a rounded stop.


### 1. Sequencing and freeze

WP-746 through WP-750 are the next durable-core packages. Until WP-750 closes, no new provider family, result-set algebra, or
performance package is admitted to the alpha gate; such a proposal is a human-review trigger. This does not stop defect fixes,
evidence closures, or documentation.

### 2. Follower mode is riffdbd with an applier where the writer was

`riffdbd --mode follower` opens the same database format, runs the unchanged ADR-0019 startup validation including the
validated-prefix checkpoint, and starts one applier where the command coordinator would start. The applier is the follower's sole
writer (PERF-007 holds on both nodes). It applies one changelog frame per storage transaction, strictly in sequence order, through
the storage-api changelog consumer ports, and advances the follower's applied frontier only after the frame is durable locally.
Every command, administration, migration, maintenance, and export surface on a follower returns the typed `RDB-REP` follower-mode
refusal; it is an application error, never a transport error.

### 3. Replication stream, capability, and handshake

Replication is one server-streaming RPC on the existing public gRPC listener, guarded by a new `ReplicateChangelog` capability
permission kind created, audited, and revoked like every capability. The handshake binds `(database id, history incarnation,
leadership epoch, requested resume sequence)`. Frames are the existing checksummed sequence-addressed changelog frames; the server
resumes from any acknowledged sequence it still retains and answers a resume below the retention watermark with the typed
`history_pruned` outcome. A stream or acknowledgement carrying a stale epoch or foreign incarnation is refused with a typed
outcome and closes the stream. The emitter reads only published snapshots and never holds the exclusive write gate (ADR-0100
section 2).

### 4. Follower reads use the existing freshness vocabulary

A follower serves compiled, projected, and discovery reads under `Causal`, `Bounded`, and `Available` with its applied frontier in
the role the local frontier plays today. `Causal` with a primary `CommitToken` waits inside the existing bounded
register-before-read discipline until the token's sequence is at or below the applied frontier. A token from another lineage or a
pre-promotion incarnation is refused, never silently satisfied. Replication lag is a sequence count, never a wall-clock duration,
and is exported as one typed `replication` health component and one statistics field on both nodes.

**Follower service and audit boundary.** Follower service composition carries no command, control-plane, or service-audit writer
executor. The follower applier remains the sole database writer. Application commands, including read-only command invocation and
outcome resolution, capability changes, contract/query/reactive-module deployment, consumer lease or cursor mutations,
installation/reimport operations, migration, maintenance, and every export operation return the typed follower-mode refusal before
execution or durable audit admission. No follower refusal allocates an application or administration sequence or creates a locally
originated authoritative record.

A follower may serve ordinary compiled, projected, catalog, and discovery reads under the existing current authorization,
redaction, bounds and freshness rules. If a standard read's current policy decision requires durable audit, the follower refuses
it with the typed follower-mode outcome before releasing data; it never removes that obligation or reports audited success.
Intrinsically audited administrative reads are likewise refused, except read-only Health and Statistics, which may disclose only
their existing authorized operational results, including the follower role, applied frontier and replication lag.

Follower Health/Statistics and explicit authenticated denials of follower reads use bounded redacted operational telemetry,
without a local durable service audit append. An authorization denial remains an authorization denial and never releases protected
data. These are explicit follower-only exceptions to SPEC §13.5; a follower supplies no durable local audit guarantee for them.
The operator handbook documents this limitation. Applications requiring durable audit for reads or denials must use the primary.
Primary audit behavior and all replicated audit bytes remain unchanged.

A follower's telemetry is non-authoritative, is never replication or recovery evidence, and cannot satisfy a policy-required
durable audit obligation. No new durable format, local audit allocator, NodeId, RPC or privilege bypass is added.

**Follower columnar views.** The exact amendment under "Follower columnar views" in SPEC §4.10 is incorporated into this
section. It permits independently validated, disposable follower V2 views under its complete admission, bounds, lifecycle,
freshness and cleanup rules, without originating follower control/generation writes or claiming the source artifact checksum.
It explicitly qualifies PRJ-005 through PRJ-009, ADR-0192 decisions 14–16, ADR-0195 decisions 4–6, and ADR-0209 decision 7.

### 5. Promotion is explicit and incarnation-fenced

Promotion is one audited administrative operation under an administrative capability. It requires proof that the old primary is
fenced: stopped, or its replication lease revoked, after which the fenced primary refuses command admission with a typed outcome.
The follower drains its received prefix, mints a new history incarnation at its applied head through the ADR-0072 stamp path,
increments the leadership epoch, and begins serving as primary. The operation's receipt records the applied sequence and the RPO
as the exact sequence delta observed at fencing. Automatic election is out of scope.

### 6. Retention fence with a hold budget

A registered follower's acknowledged frontier joins the ADR-0085 Amendment 3 fencing set as the additive `FollowerLowWater` input:
offline prune never passes it. Each registration carries a hold budget in sequences. Exhausting the budget degrades the
`replication` health component first; the fence is released only by the audited follower-retire operation or by configured expiry,
mirroring the existing retention-hold ceremony.

### 7. Changelog-derived incremental and remote backup

The exact "Archive restore receipt V3" amendment in SPEC §4.10 is incorporated here in full. Accepted by the maintainer in session
2026-09-15 ("Approve exact text"), it qualifies ADR-0050 through archive-only receipt V3 and the distinct RestoreArchivedBackup RPC;
it preserves V1/V2 bytes and exact application-sequence stopping. WP-749 owns its selection, authorization, publication and compatibility proofs.

A first-party archive consumer persists every validated frame, with its checksum and sequence range, to a configured archive sink
beside periodic offline full backups. Restore is the existing verified full backup followed by exact replay of the archived suffix
through the follower applier path, stopping at the last archived sequence or an operator-supplied earlier sequence. The offline
backup manifest V1 is unchanged; an additive `archive-manifest/v1` binds database id, history incarnation, covered sequence range,
frame digests, and the full-backup manifest digest. Recovery granularity is one commit sequence; there is no wall-clock
point-in-time promise. Filesystem and object-store sinks are the v1 targets; encryption and retention of archives are separately
configured operator policy.

### 8. Non-goals and invariants

Acknowledgement semantics, the write hot path, durable record encodings, the backup manifest, and every registry pin are
untouched. Quorum durability, automatic election, cascading followers, and multi-primary remain outside this record, as ADR-0093
section 8 requires.

## Options Considered

1. **Continue provider and performance packages first:** rejected. The
   remaining performance gap is inside the fsync floor, and every provider
   family is optional to the thesis; availability is not.
2. **Consensus-based replication (OpenRaft) as SPEC 20.6 wrote it:** rejected
   for this stage. It changes acknowledgement semantics and the commit hot
   path, contradicts `REP-001`, and the asynchronous tier delivers the
   availability step without those costs.
3. **Engine-file or page shipping:** rejected in ADR-0093; it binds the
   contract to redb internals and bypasses startup validation.
4. **Incremental backup as a separate log format:** rejected. The changelog
   is already exact, checksummed, and sequence-addressed; a second format
   would double the recovery surface.

## Consequences

- A second process class enters operations: follower health, lag, hold
  budgets, promotion, and archive sinks are new typed surfaces with audit.
- Retention gains one fencing input; a neglected follower delays pruning up
  to its hold budget, visibly and by design.
- The alpha gate widens by the WP-750 campaign, and provider work pauses
  until it closes.
- Explicitly deferred: quorum durability, automatic failover, cascading
  followers, projection-segment shipping for read scale-out, archive
  encryption, and wall-clock point-in-time recovery.

## Compatibility

Public gRPC gains one streaming RPC, one promotion operation, one follower registration/retire pair, and additive
health/statistics fields; existing messages are unchanged. Durable records, the redb layout, journal frames, changelog V1/V2
frames, and the backup manifest V1 are unchanged. The archive manifest is a new additive external format registered in the
ADR-0124 topology. Contract IR, RiffQL, and generated application surfaces are unchanged. Follower registration and hold budgets
are retained metadata added through the record-registry migration path.

## Security

The follower is a client holding a capability, not a trusted peer. The stream carries only durable bytes the primary already
persisted; redaction rules for public responses do not apply to frames, so the capability is administrative and never bindable to
an application role. Promotion and follower retirement require administrative capabilities and produce durable audit. Stale-epoch
and foreign-incarnation refusals are fail-closed. Archive sinks receive ciphertext-neutral bytes; encryption at rest is operator
policy and the manifest states whether it applied.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** No application surface
  changes. Applications cannot select a node, weaken freshness, or observe
  pre-promotion state; the only new surfaces are administrative and
  capability-gated, and follower writes are unexpressible.
- **Scale:** The design assumes shared contract vocabulary, not co-located
  storage. Frames are attributable from durable state, so emission never
  requires full-state rewrite. V1 topology is one primary and N followers per
  database; write scale-out remains the future sharding record.

## Testing

Storage conformance extends `DeleteAwareEntityFollowerV2` to every authoritative table and pins prefix exactness with the
structural-count and chain-fingerprint machinery. The process-level crash matrix gains stream-kill, applier-crash, and
archive-sink-crash arms under `storage_recovery_matrix`. The deterministic simulator gains a follower cell so seeded fault
schedules cover apply and resume. Architecture tests pin that the emitter and applier hold no `SharedRedb` handle and that no
application role can carry the replication permission. The seven obligation proofs above are the named tests; WP-750 runs them
against the app-baseline harness in a repeated kill loop and writes receipted evidence under `release/evidence/replication/`.

## Requirements and Work Packages

- **Requirements:** `REP-002` through `REP-009`
- **Defines or blocks:** WP-746 through WP-750
- **Final evidence:** WP-750

## Decision Deadline

Exact acceptance is required before WP-746 merges the replication RPC, the capability permission kind, or the handshake framing,
because each is a public protocol boundary under ADR-0124.

## Acceptance

Direction approved 2026-09-01; exact text accepted 2026-09-01. The maintainer accepted the exact text of this record in the Claude
Code session of 2026-09-01, all seven consolidation records together.

Follower service and audit amendment accepted 2026-09-15. The maintainer accepted the exact proposed amendment in session: "I
approve of these changes". The amendment above governs sections 2 and 4 and SPEC 13.5.

Follower columnar amendment accepted 2026-09-15. The maintainer approved the exact text in
`docs/architecture/WP-747-FOLLOWER-COLUMNAR-REVIEW.md` in session: "Approve the exact amendment".
Its verbatim normative text is in SPEC §4.10; WP-747 owns implementation and all required proof.
