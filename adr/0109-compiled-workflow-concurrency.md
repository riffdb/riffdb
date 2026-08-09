# ADR-0109: Compiled Workflow Concurrency and Fenced Leases

- **Status:** Accepted
- **Direction approved:** 2026-08-09
- **Exact text accepted:** Yes, 2026-08-09
- **Decision deadline:** Before WP-566 changes command grammar, transaction
  context, or scheduler interfaces
- **Requires:** ADR-0003, ADR-0005, ADR-0012, ADR-0018, ADR-0055,
  ADR-0059, ADR-0080, ADR-0093, and ADR-0095
- **Defines or blocks:** WP-566, WP-567, and WP-579

## Context

Workflow engines and agents need revision-checked transitions, claims, leases,
expiration, scheduler locks, and server-owned IDs/timestamps. Applications can
model some of these manually, but inconsistent conventions recreate lost
updates, stale lease holders, duplicate scheduling, caller clocks, and
unfenced work.

RiffDB must make the correct pattern expressible without running application
code inside a transaction or making the scheduler a privileged mutation path.

## Proposed Decision

The contract language adds first-class, compiler-lowered workflow declarations
that remain ordinary command semantics.

### Revision-checked transitions

An entity or aggregate may declare a workflow state field and legal directed
transitions. A transition command names its allowed source states, destination,
required exact entity revision, and declared stale/illegal outcomes. The
compiler injects no hidden read: source contains the exact binding and
revision/state requirements, and explain output shows them. Commit-time
transaction-current validation makes lost-update omission impossible.

Generated clients require the observed revision for revision-sensitive
commands and return the successor revision. There is no unconditional update,
last-write-wins flag, or retry that substitutes a newer revision on the
caller's behalf.

### Leases, claims, and fencing

A workflow may declare a lease over one aggregate-owned entity with:

- optional owner identity;
- service-owned expiration timestamp;
- monotonically increasing nonzero fencing token;
- optional bounded attempt count; and
- declared claim, renew, release, and expire commands.

Claim succeeds only when unowned or expired at the admitted transaction time,
increments the fencing token, and returns owner, expiration, revision, and
token. Renew and release require the exact current owner, revision, and fencing
token. Every command that performs lease-protected work must declare and check
that token; a stale holder cannot write after another claimant succeeds.

Expiration is a normal compiled command. The passage of wall time alone does
not mutate state or emit an event. Reads may report a lease as temporally
expired relative to an authorized service time observation, but authoritative
availability changes only through claim/expire command validation.

### Service-owned identity and time

Contracts may declare command values as `service uuid_v7` or
`service transaction_time`. Those values are supplied by the admitted
deterministic transaction context, become part of the persisted command
evidence/outcome, and replay exactly. Callers cannot override them. Caller-owned
business IDs remain explicit inputs where the contract chooses them.

Lease durations are typed bounded durations, not absolute caller timestamps.
Expiration is computed by checked arithmetic from transaction time. Clock
rollback, overflow, excessive duration, and unavailable trusted time fail
closed before mutation.

This complies explicitly with architecture boundary 4. The service may observe
an authorized UUID/time source before deterministic evaluation, but it must
seal the resulting value into the admitted transaction context and persisted
command evidence. The command runtime receives only that value; it performs no
operating-system clock call, randomness call, network access, filesystem
access, or process-global mutation. Retry and recovery reuse the persisted
observation byte-for-byte.

An aggregate-local **lease fencing token** is not ADR-0093's database-history
incarnation and is not a replication leadership epoch. They use distinct
types, namespaces, encodings, and diagnostics. A future promoted leader must
validate both the routing/leadership epoch appropriate to the request and the
aggregate's current lease token; a token issued before promotion does not by
itself authorize or route a post-promotion write. Promotion never resets or
reinterprets an aggregate's monotonically increasing lease token.

### Scheduler ownership

The scheduler is a bounded service worker that discovers eligible work through
named queries or durable event subscriptions and invokes exact compiled
commands under a dedicated symbolic role. It has no entity editor, storage
handle, lock table, generic callback, or hidden transaction. Scheduler locks
are ordinary fenced lease entities/commands and survive restart through normal
state.

Each scheduled attempt derives its idempotency identity from the schedule
definition, logical due instant, target key, and attempt kind. A crash after
command commit and before checkpoint replays the original outcome. The
scheduler provides at-least-once attempts and fenced state transitions; it does
not promise exactly-once external effects.

Work selection, in-flight claims, timers, retries, and wakeups are bounded.
External effects continue to use domain events/outbox intent after the command
commit.

## Options Considered

1. **Document optimistic locking conventions:** rejected because omission
   remains expressible in generated application code.
2. **Expose a generic compare-and-swap or lock API:** rejected because it
   bypasses named command outcomes and domain invariants.
3. **Run workflow callbacks in the database transaction:** rejected because
   they are nondeterministic and unbounded.
4. **Compiler-lowered transitions and fenced lease commands:** proposed because
   every write remains typed, idempotent, local, and auditable.

## Consequences

- Workflow adapters share one safe concurrency model instead of bespoke CAS.
- Contracts become more explicit and generated methods require revisions and
  fencing tokens where correctness needs them.
- Expiration requires a command/scheduler attempt; it is not magical wall-clock
  mutation.
- Distributed scheduler leadership and cross-partition locks remain deferred.

## Compatibility

Workflow grammar, IR, plan hashes, generated signatures, service-owned value
tags, and outcome schemas require additive versioned successors. Existing
commands and transaction-context values retain their meanings. No current row
is retroactively lease-protected without migration.

## Security

Scheduler roles contain only exact queries/subscriptions and commands. Owner,
revision, token, and timing diagnostics are authorization-filtered and bounded.
Lease possession is state, not authority: every invocation still authenticates
and authorizes current capability facts.

## Standing Design Tests

- **Interface safety:** generated callers cannot omit a required revision or
  fence, override service time/ID, renew another owner, or ask for a generic
  lock/update. Unsafe transition shapes fail compilation.
- **Scale:** leases and schedules are partition-local, query/index driven, and
  bounded. No global scheduler lock or full-state scan is introduced; later
  partition leaders can own local schedulers.

## Testing

- Compiler negative corpus for omitted revisions, illegal transitions, caller
  time, excessive durations, cross-partition leases, and unindexed schedules.
- Deterministic schedules for claim/renew/release/expire races and stale fences.
- Clock rollback/overflow and UUID/time replay tests.
- Process crash at selection, claim, business command, event, and checkpoint.
- Workflow-shaped adapter acceptance for MLflow runs and Woodpecker pipelines.

## Requirements and Work Packages

- **Provisional requirements:** `WF-001` through `WF-014`, to be added to
  `SPEC.md` only after exact acceptance.
- **Defines or blocks:** WP-566, WP-567, and WP-579.
- **Final evidence:** WP-567 and WP-579.

## Decision Deadline

Exact acceptance is required before workflow grammar/IR, lease state,
service-owned values, scheduler operations, or generated signatures change.
