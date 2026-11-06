---
adr: "0337"
title: Quorum-Acknowledged Replication and Epoch-Fenced Automatic Failover
status: accepted
tier: guarantee
date: 2026-11-05
accepted: 2026-11-05
acceptance: 'maintainer, in session, 2026-11-05: approved implementation_plan.md (ADR-0335 through ADR-0337)'
requires: [ADR-0093, ADR-0178, ADR-0310, ADR-0335, ADR-0336]
amends: [ADR-0178, ADR-0310]
supersedes: []
requirements: [REP-002, REP-003, REP-004, REP-005, REP-006, DRV-001, DRV-005, ID-001, ID-002]
packages: [WP-904, WP-905, WP-906]
obligations:
  - id: OBL-0337-1
    package: WP-904
    description: "Wait for at least K distinct registered voter followers to durably acknowledge a committed sequence under commit_acknowledgement = \"quorum\" and fail closed with OutcomeUnknown on timeout."
    proof: quorum_commit_waits_for_k_follower_acks_and_returns_unknown_on_timeout
  - id: OBL-0337-2
    package: WP-905
    description: "Persist promised_leadership_epoch durably on voters, fence out stale epochs on follower appliers, and promote the highest-frontier candidate via QuorumFenceCertificateV1 when the old primary is unreachable."
    proof: epoch_vote_quorum_fences_stale_primary_and_elects_highest_frontier_candidate
  - id: OBL-0337-3
    package: WP-906
    description: "Self-fence the primary before follower election_timeout expires when voter quorum lease contact is lost, and automatically elect and promote the highest-frontier follower with zero lost quorum commits."
    proof: primary_lease_expiry_self_fences_and_auto_failover_promotes_new_primary
review_triggers:
  - "A quorum-configured primary acknowledges a command as committed before the configured quorum of registered voter followers durably acknowledges its commit sequence and frame hash."
  - "A quorum acknowledgement timeout rolls back local durable state or returns a synthetic rejection instead of failing closed with CommitStatusUnknown."
  - "A voter grants an epoch vote for leadership_epoch E' without durably persisting promised_leadership_epoch = E' and fencing out frames from epochs < E'."
  - "A primary continues serving writes or linearizable reads after its monotonic quorum lease expires without self-fencing."
---
# ADR-0337: Quorum-Acknowledged Replication and Epoch-Fenced Automatic Failover

## Context

`ADR-0178` and `ADR-0310` established single-primary changelog replication (`ReplicateChangelog`), follower read service, explicit primary fencing (`FenceReplicationPrimary`), operator-triggered promotion (`PromoteFollower`), and standby-aware client routing (`standby_endpoints`). However, three high-availability limitations remain:

1. Primary commits acknowledge immediately after local journal persistence (`ADR-0101`), before any follower acknowledges receipt (`RPO > 0` on primary loss).
2. `promote_follower` in `daemon_promotion.rs` requires connecting to the old primary over TLS to obtain `PrimaryFenceSourceEvidenceV1`. If the old primary host crashes hard or is network-unreachable, promotion fails with `FenceUnavailable`.
3. Failover requires manual operator execution (`riffdb follower failover`); `REP-005` deferred automatic failover until quorum fencing and lease safety were defined.

## Decision

1. **Deployment-Configured Quorum Commit Durability (`commit_acknowledgement = "local" | "quorum"`):**
   - Primary replication configuration MAY declare `commit_acknowledgement = "quorum"` with `quorum_ack_followers: usize` ($K \ge 1$) over registered voter followers and a bounded `quorum_ack_timeout_ms` ($100\text{ ms}..=30,000\text{ ms}$). Per `AGENTS.md` Boundary 11, applications cannot opt out of configured quorum durability per request.
   - Under `commit_acknowledgement = "quorum"`, the single primary writer commits locally to the authoritative journal as normal (`PERF-007`), publishes the frame to `PublishedReplicationSource`, and holds external command outcome release until at least $K$ distinct registered voter followers acknowledge durable local receipt at or above `(history_incarnation, leadership_epoch, commit_sequence, frame_hash)` via `AcknowledgeReplicationFollower`.
   - If the quorum barrier times out or the primary loses its lease before $K$ voter acknowledgements arrive, `riffdbd` MUST return `CommitStatusUnknown` (never rolling back local durable state and never reporting un-replicated success) so idempotent retry (`ADR-0005`) resolves the outcome safely.

2. **Durable Voter Epoch Promises and Quorum Epoch-Fence Certificates (`QuorumFenceCertificateV1`):**
   - Every node persists `promised_leadership_epoch: u64` in its durable replication metadata.
   - Add an administrative replication RPC `RequestReplicationEpochVote(RequestReplicationEpochVoteRequest) -> RequestReplicationEpochVoteResponse`:
     - A voter grants a vote for `target_leadership_epoch = E'` to candidate `candidate_follower_id` iff:
       1. `E' > promised_leadership_epoch`,
       2. The candidate's `(history_incarnation, applied_commit_sequence, applied_frame_hash)` is at least as advanced as the voter's own durable frontier, and
       3. The voter durably persists `promised_leadership_epoch = E'` before returning `VoteGranted`, immediately fencing out any subsequent `ReplicateChangelog` frames or heartbeats carrying `leadership_epoch < E'`.
   - Generalize promotion fence proof in `PromoteFollower` to accept either:
     - **Direct Primary Fence (`PrimaryFenceSourceEvidenceV1`):** existing zero-lag switchover proof when the old primary is reachable, OR
     - **Quorum Epoch Fence (`QuorumFenceCertificateV1`):** a set of authenticated `EpochVoteGrantV1` records for `target_leadership_epoch = E'` from a majority quorum of the configured voter membership (including the candidate itself), enabling safe promotion with **zero RPO** (for quorum-acknowledged commits) even when the old primary host is dead or unreachable.

3. **Monotonic Primary Lease Self-Fencing and Automatic Failover Coordinator:**
   - When `auto_failover = true` is configured with a voter membership (`voters = [...]`, `lease_duration_ms`, `election_timeout_ms`):
     - **Primary Lease Self-Fencing:** The primary tracks monotonic `Instant` timestamps of voter acknowledgements/heartbeats. If the primary fails to refresh acknowledgements from a voter quorum within `lease_duration_ms`, it MUST immediately transition its local replication role to `PrimaryFenced` before any follower's `election_timeout_ms` (> `lease_duration_ms`) can elapse.
     - **Automatic Follower Election & Promotion:** When a voter follower detects no primary frames/heartbeats within `election_timeout_ms` (plus deterministic priority rank offset so the highest-frontier voter bids first), it increments `target_leadership_epoch`, collects `QuorumFenceCertificateV1` via `RequestReplicationEpochVote`, and executes `promote_follower` automatically.
     - Clients configured with `standby_endpoints` (`ADR-0310`) automatically fail over to the newly promoted primary.

## Options considered

1. **Multi-leader or per-command Paxos/Raft consensus log replacing the storage engine:** rejected per `AGENTS.md` Boundary 10 and because RiffDB's single ordered writer + V3 changelog stream already provides a total order per leadership epoch.
2. **Allow unreachable-primary promotion without voter epoch fencing (`--force`):** rejected because an unfenced partitioned primary could accept divergent writes, violating interface safety and zero-RPO guarantees.
3. **Quorum follower acknowledgement barrier + durable voter epoch-fence certificates (`QuorumFenceCertificateV1`) + monotonic primary lease self-fencing:** chosen because quorum intersection guarantees the elected follower holds all quorum-acknowledged commits and lease self-fencing prevents split-brain writes.

## Consequences

- `commit_acknowledgement = "quorum"` provides RPO = 0 across voter quorum survivals while preserving `commit_acknowledgement = "local"` for asynchronous deployments.
- Operators and the automatic failover coordinator can safely promote a follower when the old primary machine is dead/unreachable using `QuorumFenceCertificateV1`.
- `REP-005` is amended to permit epoch-fenced automatic failover when `auto_failover = true` is explicitly configured with a voter quorum.

## Standing design tests

- **Interface safety:** Callers never opt out of quorum durability per command; timed-out quorum waits return `CommitStatusUnknown`; stale-epoch primaries and followers fail closed.
- **Scale:** Quorum acknowledgement uses event-driven watch notifications without polling loops; lease timers use monotonic clocks.

## Checks

- Quorum commit acknowledgement tests: success with $\ge K$ voter acks, `CommitStatusUnknown` timeout when $< K$ voters respond, and idempotent outcome resolution after retry.
- Unreachable-primary promotion and automatic failover tests: kill primary after quorum commit, verify automatic `QuorumFenceCertificateV1` election promotes the follower with zero lost quorum commits, and verify old primary rejects writes (`PrimaryFenced`) and its stale epoch is rejected by voters.
