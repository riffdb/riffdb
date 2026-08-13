# Workflow Transitions and Fenced Leases

RiffDB workflow operations are compiled commands, not a lock API. They use the
same authorization, idempotency, provenance, commit, and recovery path as every
other application mutation. There is no generic compare-and-swap, entity
editor, caller clock, or scheduler bypass.

For the bounded worker protocol that safely composes these commands with named
queries or durable events, see [Bounded Workflow Schedulers](SCHEDULERS.md).

## Declare the workflow state

```riff
enum WorkState { Queued, Running, Complete }

entity WorkItem {
  key (organization_id: uuid, work_id: uuid)
  field state: WorkState
  field lease_owner: optional<uuid>
  field lease_expires_at: optional<timestamp>
  field lease_fence: u64
  field lease_attempts: u64
}

workflow WorkLifecycle {
  entity WorkItem
  state state
  initial Queued
  transition Start from (Queued) to Running
  transition Finish from (Running) to Complete
  lease execution {
    owner lease_owner
    expires_at lease_expires_at
    fencing_token lease_fence
    attempts lease_attempts
    duration_seconds (5, 900)
  }
}
```

`initial` is the only ordinary-create path for a workflow state. The compiler
injects the named enum value into every ordinary or bounded-collection create;
the command has no input or `set` operation that can choose a different state.
Without `initial`, an ordinary create cannot construct the required workflow
state field. Portable restoration is a separate, operator-authorized operation
and does not widen normal application commands.

A declared transition may return to the same state. This is useful for
revision-checked operations such as rotating a session token while the session
remains `Active`. A self-transition still requires the exact observed revision,
checks the declared source state, returns the declared stale/illegal outcomes,
and advances the entity revision only with the atomic mutation.

The owner and expiry fields must be `optional<uuid>` and
`optional<timestamp>`. The fence is `u64`; the optional attempt field is also
`u64`. Lease fields must be distinct non-key fields. Duration bounds are
inclusive, nonzero seconds and cannot exceed 86,400 seconds.

New rows receive their workflow state from `initial`. They initialize the
optional owner and expiry to null and must explicitly initialize the fence and
attempt counters. The first successful claim advances the fence from zero to
one.

## Claim work

```riff
command ClaimWork {
  input request_key: string<128>
  input organization_id: uuid
  input work_id: uuid
  input owner_id: uuid
  input duration: u64
  input expected_revision: u64
  idempotency_key request_key

  mutate WorkItem(organization_id, work_id) as work else Missing {}
  lease claim execution on work owner owner_id duration_seconds duration revision expected_revision
    stale StaleRevision {}
    unavailable AlreadyClaimed {}
    invalid InvalidDuration {}
    exhausted LeaseCounterExhausted {}

  return Claimed { work: work }
}
```

Claim requires an exact observed entity revision. It succeeds only for an
unowned lease or one whose expiry is at or before the admitted transaction
time. Success increments the nonzero fence and, when declared, the attempt
counter. Counter or timestamp overflow returns the declared `exhausted`
outcome without mutation.

The owner, duration, revision, and fencing-token expressions used by lease
operations must be direct, typed command inputs. RiffDB rejects computed or
implicitly substituted values. Generated application bindings therefore make
the exact concurrency evidence required method parameters.

On the declared success outcome, generated clients also return a bounded
`workflow_revisions` collection. Each item names the source binding and its
exact successor entity revision. Callers feed that revision into the next
transition, renewal, release, expiration, or fenced business command; they do
not re-read the entity or guess from a commit sequence. Non-success outcomes
return an empty collection. Same-key replay returns the identical successor
revision because the accepted input and persisted outcome are identical.

For example, a successful `ClaimWork` called with `expected_revision = 7`
returns `{ binding: "work", revision: 8 }` alongside `Claimed`. Revision
overflow fails closed as an invalid generated response and never wraps.

## Renew, release, expire, and protect work

The remaining operations form a closed family:

```riff
lease renew execution on work owner owner_id fencing_token token duration_seconds duration revision expected_revision
  stale RenewStale {} invalid WrongHolder {} expired LeaseExpired {} exhausted LeaseCounterExhausted {}

lease release execution on work owner owner_id fencing_token token revision expected_revision
  stale ReleaseStale {} invalid WrongHolder {}

lease expire execution on work revision expected_revision
  stale ExpireStale {} active LeaseStillActive {}

lease fence execution on work owner owner_id fencing_token token revision expected_revision
  stale WorkStale {} invalid WrongHolder {} expired LeaseExpired {}
```

- `renew` requires the current owner, fence, revision, and an unexpired lease;
  it computes a new expiry from transaction time and the checked duration.
- `release` requires the current owner, fence, and revision; it clears owner and
  expiry but never rolls the fence backward.
- `expire` requires the exact revision and succeeds only after the recorded
  expiry. Time passing alone never mutates authoritative state.
- `fence` performs no lease-field mutation. It proves that ordinary protected
  work is still held by the exact owner, fence, and revision before the other
  command effects can commit.

Every command that mutates an entity with a declared lease must contain exactly
one lease operation for that binding. Omitting the fence is a compiler error.
This prevents a generated caller from accidentally writing around the lease.
Creation remains possible so the initially unowned row can be established.

## Time, retries, and authority

Lease expiry uses only the service-observed transaction time sealed at command
admission. The deterministic runtime never reads the operating-system clock or
randomness. Retry and recovery reuse the persisted observation, so the same
idempotency identity cannot acquire a different expiry or service-owned value.

A lease is state, not authority. Possessing an owner ID or fencing token never
grants command permission. Every claim, renewal, release, expiration, and
protected action is authenticated and authorized under current capability
facts. Avoid placing lease evidence in logs or public diagnostics.

Aggregate lease fences are also distinct from database-history incarnation and
future replication leadership epochs. They are never interchangeable and a
future promoted leader must validate both routing/leadership evidence and the
aggregate's current fence.

## Failure model

All business failures are declared outcomes and commit with zero mutations.
Concurrent changes are transaction-current dependencies: RiffDB either commits
the exact checked operation, reevaluates within its bounded retry policy, or
returns the declared stale outcome. It never silently substitutes a newer
revision or token for the caller.
