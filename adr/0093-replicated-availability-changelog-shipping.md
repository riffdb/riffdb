# ADR-0093: Replicated Availability via Authoritative Changelog Shipping

- **Status:** Accepted
- **Date:** 2026-08-04
- **Decision owners:** RiffDB maintainers
- **Related:** ADR-0082 (single total order), ADR-0083 (commit records carry no
  post-images; entity supersession chains), ADR-0061 (acknowledgement strictly
  after durability), ADR-0085 + amendments (retention watermark and fencing;
  validated-prefix checkpoint), ADR-0086 (freshness classes, incarnation-bound
  frontiers and commit tokens), ADR-0019 (startup validation), ADR-0080
  (partitioned events and durable consumers — rejected here as a replication
  substrate)

## Context

RiffDB today runs on exactly one node. The biggest honest gap in the
architecture review is availability: a lost disk is a restore-from-backup
event with data loss up to the last backup, and a crashed host is an outage
for as long as the host is down. Every other pillar of the product thesis —
safe transactions, migrations, subscribers, projections, search — is native
and opinionated; durability beyond one machine is the one pattern the
average multi-tenant SaaS still has to solve somewhere else.

Three properties of the existing contracts make replication unusually
tractable here, and this record is shaped around them:

1. **There is one total order** (ADR-0082). Every durable write is bound to a
   commit sequence. A replica that holds an exact prefix of that order holds
   a valid database — the same prefix property the validated-prefix
   checkpoint (ADR-0085 A1) and offline retention (ADR-0085 A2) already
   depend on and verify.
2. **Freshness is already a first-class, node-shaped vocabulary**
   (ADR-0086). `ProjectionFrontier` and `CommitToken` are opaque,
   incarnation-bound, and fail closed on incarnation mismatch. A token
   minted by one process is meaningful to another process holding the same
   database lineage — the contract was written for a single node but nothing
   in it assumes one.
3. **Acknowledgement semantics are settled** (ADR-0061): a client sees
   success only after local durability. Asynchronous replication does not
   touch that boundary, so v1 changes no hot-path semantics at all.

The maintainer's standing scale test applies: *does this design assume
co-located authoritative storage, single-node memory, or full-state
rewrite?* Each section below answers it.

This record deliberately contracts the **asynchronous** tier only. A
synchronous/quorum tier changes acknowledgement semantics and the commit hot
path; it is named as a future amendment with its required safeguards
enumerated (§8), not contracted here.

## Decision

### 1. The authoritative changelog: exact, gap-free, sequence-attributed

The unit of replication is the **authoritative changelog entry**: for commit
sequence *s*, the complete, checksummed set of durable record writes bound to
*s* across every authoritative table — commits, events, entities (via the
ADR-0083 supersession chain, which makes per-sequence entity writes
attributable), administration and audit records, registry migrations,
capabilities, outbox intents. The changelog is a derived view of durable
state, not a second durable structure on the primary: no new write-time
obligation enters the commit path, and no durable format changes. How the
emitter produces entries (tailing durable tables by sequence, an in-memory
ring at the group-commit completion edge with chain-walk fallback on resume,
or both) is a private implementation phase in the PERF-004 sense; the
contract requires only that the stream is **exact** (byte-faithful to the
primary's durable records), **gap-free** (a strict prefix, always), and
**resumable from any acknowledged sequence**.

*Scale test:* the changelog is defined against the total order, not against
node-local memory; entries are attributable from durable state, so emission
never requires full-state rewrite.

### 2. A follower is a read-only riffdbd with an applier where the writer was

A **follower** is the same `riffdbd` binary in follower mode: it applies
changelog entries atomically, one commit sequence per storage transaction,
strictly in order. The applier is the follower's sole writer — the
single-writer property (PERF-007) holds on both nodes by construction. All
command and administration surfaces on a follower refuse with a typed
follower-mode outcome (agent-legible, not a transport error). Startup
validation (ADR-0019, including the validated-prefix checkpoint) runs on
followers unchanged: a follower is always a database that would pass primary
startup at its applied head.

### 3. Reads on a follower speak the existing freshness vocabulary

A follower serves reads — compiled, projected, and discovery — under the
same `Causal` / `Bounded` / `Available` policies, with the follower's
**applied frontier** in the role the local frontier plays today:

- `Causal` with a `CommitToken` waits (bounded, using the existing
  register-before-read wait discipline) until the token's sequence is at or
  below the applied frontier. **Read-your-writes across nodes is therefore
  the existing token round-trip, unchanged**: commit on the primary, carry
  the token, read anywhere.
- `Bounded(lag)` and `Available` behave per ADR-0086 against the follower's
  frontier.

Because tokens and frontiers are incarnation-bound and fail closed, a token
from a different lineage or a pre-promotion incarnation is refused, never
silently satisfied. Replication **lag is measured in sequences**, never wall
clocks (the architecture's no-clocks rule), and is exported as a typed,
queryable quantity on both nodes.

*Scale test:* the read path assumes shared *contract vocabulary*, not
co-located storage. The separately ledgered **projection changelog** for
remote read compute (scale directive) remains a distinct, complementary
future: this record replicates full authoritative state for availability;
that one ships derived segments for cheap read scale-out. Neither subsumes
the other and the two must not be conflated.

### 4. Bootstrap and retention fencing

A follower seeds from the existing backup format (unchanged — replication
adds nothing to the backup allowlist) or from a full sync stream, then tails
the changelog from its verified head. The follower's **acknowledged
frontier** becomes an additional fencing input to the retention watermark
minimum (ADR-0085 A2's fencing set grows by one): the primary never prunes
history a registered follower has not durably applied. Because a dead
follower must not pin retention forever, a follower registration carries an
explicit hold budget: exhausting it degrades typed health first and releases
the fence only by operator action or configured expiry — the same
hold-with-ceremony shape retention holds already have.

### 5. Promotion: incarnation is the fence

Promotion of a follower to primary is an explicit administrative operation,
never automatic in v1:

1. The old primary is fenced first (stopped, or its replication lease
   revoked — §6). A fenced primary refuses further command admission with a
   typed outcome.
2. The follower completes applying its received prefix, then **mints a new
   database incarnation** at its applied head.
3. Every incarnation-bound artifact from the old lineage — frontiers,
   commit tokens, replication cursors — now **fails closed** by the existing
   ADR-0086 contract. No stale read is ever silently satisfied against the
   new lineage; no old-primary stream is ever accepted by anyone.

The data-loss window (RPO) is exactly the sequences above the follower's
applied frontier at fencing time — an honest, typed, inspectable number, not
a marketing property. Clients holding unacknowledged or
outcome-unknown commands from the fenced primary recover through the
existing idempotency and unknown-result machinery against the new primary.

### 6. Transport, authorization, and the leadership epoch

Replication is a dedicated streaming RPC surface on the server, guarded by a
new administrative **replication capability** (created, audited, and revoked
like every capability; a follower is a client, not a trusted peer). The
stream handshake binds `(database id, incarnation, leadership epoch)`;
epochs increase at every promotion; a stream or acknowledgement carrying a
stale epoch is refused with a typed outcome. Frames are checksummed and
sequence-addressed; resume is from any acknowledged sequence. v1 topology is
one primary and N followers (read replicas and standby candidates are the
same mode; promotion eligibility is configuration); cascading followers and
automatic election are out of scope.

*Scale test:* authorization at the replication boundary reuses capabilities
rather than assuming a co-located authorizer, per the distributed-auth
directive.

### 7. What v1 explicitly does not change

- Acknowledgement semantics (ADR-0061) — untouched; async replication is
  invisible to commit latency.
- The backup format and durable-state enumeration — untouched.
- Any durable record encoding, schema hash, or registry pin — untouched.
- The write hot path — the emitter reads committed state; it never holds the
  exclusive write gate.

### 8. The synchronous tier is a named amendment, not a v1 deliverable

Quorum-acknowledged durability (RPO zero) requires, at minimum: a
per-database explicit durability policy (never a global toggle), an
acknowledgement boundary redefinition with the same rigor ADR-0061 applied
to the local one, leader leases with proven fencing (the epoch machinery of
§6 is deliberately shaped to be its substrate), interaction analysis with
ADR-0061's deferred non-durable chaining, and crash-evidence across both
nodes. It must arrive as an amendment to this record carrying all five, and
its cost lands on the commit hot path this codebase spent July measuring —
which is precisely why it is not being contracted casually here.

## Consequences

- A second process class (follower) enters the operational story: health,
  lag, and hold budgets are new typed surfaces; promotion is a new
  administrative ceremony with audit records.
- Retention gains a fencing input; a neglected follower can delay pruning up
  to its hold budget — visible, bounded, and by design.
- The changelog emitter and applier are new correctness-critical code on the
  read side of durable state; their acceptance evidence is prefix-exactness
  (below), leaning on the structural-count and chain-fingerprint machinery
  that already exists for startup validation.
- The freshness vocabulary becomes load-bearing across processes; the
  fail-closed incarnation contract graduates from defensive posture to the
  primary safety mechanism of failover.

## Rejected alternatives

- **Engine-file or page shipping** (redb-level replication): binds the
  replication contract to engine internals ADR-0058 deliberately keeps
  evidence-gated and swappable, and bypasses the startup-validation story
  that makes an applied prefix trustworthy.
- **Reusing durable event consumers (ADR-0080) as the transport**: consumers
  carry events; a promotable standby needs administration, audit, registry,
  capability, and entity state too. Partial fidelity is the wrong kind of
  replica — it looks like a database until promotion.
- **WAL-first replication** (the deferred Option-B commit log): would
  require the backup-allowlist, STO-012, and read-fence amendments the perf
  plan already declined to open without evidence; shipping the changelog
  derived from durable state gets availability without reopening storage
  contracts.
- **Multi-primary**: permanently rejected — ADR-0082's single total order is
  the foundation of nearly every contract above; writes converge on one
  sequencer per database. Scale-out of writes is the (future) sharding ADR's
  problem, org-keyed, not a replication mode.
- **Snapshot-only cold standby**: an RPO of "since the last backup" is the
  status quo this record exists to retire.

## Acceptance criteria (feasibility prototype + evidence)

1. **Prefix exactness:** primary and follower, driven by a real workload
   (the app-baseline harness), compared at an identical applied sequence by
   the structural-count and chain-fingerprint machinery: byte-faithful
   agreement across all authoritative tables.
2. **Gap-free resume:** kill the stream mid-flight repeatedly; the follower
   resumes from its acknowledged sequence with no gap, no duplicate apply,
   and passes full startup validation afterward.
3. **Promotion drill:** fence the primary under load, promote the follower;
   a pre-promotion commit token is refused fail-closed by the new
   incarnation; a post-promotion commit and read round-trip succeeds; RPO
   equals the measured sequence delta.
4. **Retention fence:** with a lagging follower registered, offline prune
   refuses to pass its acknowledged frontier; after the follower catches up,
   the same prune proceeds.
5. **Follower reads:** `Causal` on the follower with a fresh primary token
   waits then serves; results byte-equal to the primary at the same
   frontier; typed follower-mode refusal for writes.
6. **No hot-path regression:** PERF-005/PERF-008 measurements with the
   emitter enabled are within noise of current main.

## Acceptance

Accepted by the maintainer on 2026-08-04, as written: the asynchronous tier
(authoritative changelog shipping, follower mode, incarnation-fenced
promotion, retention fencing with hold budgets, capability-authorized
replication transport) is contracted; the synchronous/quorum tier remains a
named future amendment carrying the five §8 safeguards.
