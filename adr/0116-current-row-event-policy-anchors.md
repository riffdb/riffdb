# ADR-0116: Current-Row Policy Anchors for Durable Events

- **Status:** Proposed
- **Direction requested:** 2026-08-11
- **Decision deadline:** Before WP-572 enables protected event consumption
- **Requires:** ADR-0007, ADR-0038, ADR-0055, ADR-0080, ADR-0111, and ADR-0114
- **Amends if accepted:** ADR-0111 Amendment 1
- **Defines or blocks:** WP-572, WP-573, WP-578, and WP-579

## Context

ADR-0111 forbids inferring event authority from payload field names and keeps
protected event streams fail-closed until the compiler owns an explicit event
policy anchor. The remaining decision is how an event identifies the row whose
current policy controls delivery, how replay behaves after policy or row
changes, and how hidden events affect checkpoints and cursors.

Persisting an allow decision at emission would survive later ACL revocation.
Filtering after delivery selection would leak hidden event counts and ordering.
General event predicates would create a second policy language. The alpha
therefore needs one narrow anchor that reuses the compiled entity row policy.

## Proposed Decision

### One explicit current-row anchor

A protected event declaration may name exactly one source entity and provide a
complete canonical mapping from that entity's partition/key fields to typed
event payload fields. Illustrative syntax, frozen by WP-572 before use:

```riff
event TicketCreated {
  partition_by (organization_id)
  policy_anchor current Ticket(
    organization_id: organization_id,
    ticket_id: ticket_id,
  )

  organization_id: uuid
  ticket_id: uuid
}
```

The compiler resolves all names and emits the entity stable identity, exact
key projection, selected read-policy identity, required principal-fact schema,
relationship/index dependencies, partition route, and bounded cost into the
contract/event IR. Payload field names alone have no meaning. Every mapped
field must exist, have the exact key type, be emitted on every path, and supply
the event's declared partition route. Partial, optional, dynamic, cross-
partition, multiple, or unindexed anchors are compile errors with source spans.

The alpha has no separately authored event-policy language. Events that cannot
be governed by a current entity row—including deletion events after the row is
gone—remain unavailable to ordinary protected consumers. Whole-application
operator export may carry them only under ADR-0115 authority. A later immutable
event policy requires a separate accepted ADR.

### Emission and durable identity

Command compilation proves that every emit site supplies the complete anchor
key. The event record persists a versioned anchor descriptor containing the
originating contract lineage/version/bundle hash, event stable identity/version,
source entity identity, selected policy identity, and canonical entity key.
It never persists principal facts or an allow decision. The descriptor and
event are atomic with the command as part of the existing event record's
versioned successor; old event records without an anchor remain fail-closed on
protected streams.

The anchor key must encode to the same partition as the event. Contract
successors may add new anchored event versions but cannot reinterpret the
anchor of retained events. Descriptor/IR/fixture and deployed bundle rotation
uses the accepted exact-identity ceremony.

### Delivery uses current authority and current row

Before an event can affect `next`, a gRPC stream, a contextual subscription,
MCP work availability, hydration, batch size, or caller-visible cursor, the
service:

1. reloads the current capability and exact V4 role/policy/fact binding;
2. resolves the retained exact contract/event anchor descriptor;
3. reads the anchored entity and bounded indexed relationship evidence from
   one authoritative snapshot at or after the event commit sequence;
4. evaluates the shared row-policy evaluator; and
5. revalidates capability revision and policy proof before release.

A missing row, missing retained contract/policy, mismatched key/partition,
missing relationship evidence, changed role binding, or denied policy is
indistinguishable absence for that consumer. It never falls back to payload
inspection or emission-time authority. Contextual hydration starts only after
the trigger event passes this check; each hydrated query independently applies
its own row policy.

Current-row semantics are deliberate: ownership/ACL changes alter future
delivery and replay, and row deletion makes retained events invisible to
ordinary consumers. Event durability is not a promise that every principal
will remain authorized to observe an event forever.

### Alpha adapter deletion-event audit

The 2026-08-11 alpha-gate audit found no requirement for a protected deletion
event in any of the four adapter workloads:

- Payload's WP-573/final-gate corpus requires document creation, owner/team/
  public ACLs, draft/transfer/revocation behavior, queries, search, aggregates,
  and live updates, but not a document-deletion notification.
- MLflow requires experiment/run policy, metric and artifact relationships,
  lifecycle transitions, dashboards, and events, but not a run- or experiment-
  deletion event.
- OpenFGA requires bounded tuple writes/deletes and indexed reads, but its
  alpha adapter manifest declares no protected event stream.
- Woodpecker's protected `PipelineTransitions` stream contains
  `PipelineStartedEvent`; it does not contain a deletion event.

The bulk conformance fixture's `DeleteRestrictParents` command tests
transaction-current restrict-delete behavior and emits no event. It therefore
does not contradict this audit. The alpha may rely on current-row anchors
without making a gate workload unimplementable. Adding a protected deletion
event to any gate adapter is a human-review trigger and requires an accepted
immutable-event-policy ADR before the workload or role can be widened.

### Hidden-event checkpoint and replay semantics

The server scans one partition-ordered retained stream under existing bounded
work limits. Hidden events consume internal scan work but not returned item
limits, inflight leases, payload bytes, availability notifications, or visible
counts. The durable consumer checkpoint may advance across hidden events only
as part of an atomic server-owned skip transition bound to the current
capability revision and anchor-policy identity. The client never receives a
hidden event ID, sequence, or gap.

If policy denial later becomes allowance, an already checkpointed hidden event
does not reappear automatically. Seeking/replaying from an earlier retained
cursor re-evaluates current policy and may reveal it then. Narrowing authority
can only remove future/replayed visibility. Opaque cursors bind stream,
consumer, contract/reactive module, partition, capability revision, and policy
identity; a binding change yields the existing typed authorization-change or
reset outcome rather than splicing identities.

`events.next` must bound both returned items and examined candidates. Reaching
the candidate ceiling with no visible item returns a typed bounded-progress
result carrying only an opaque resumable cursor; it does not reveal how many
events were hidden. Wakeup notifications are emitted only when at least one
currently authorized item is known within the same bounded check, or remain a
non-authoritative hint requiring `next` to revalidate.

### Revocation and live consumers

Capability revocation, fact narrowing, role/policy rotation, or relationship
change closes outstanding delivery leases before further release. Unacked work
is not acknowledged by closure and is reconsidered under current authority on
the next bounded delivery attempt. A command caused by an event still derives
its idempotency and causation identity from the stable event ID; policy
filtering does not alter event identity or at-least-once semantics.

## Compatibility

The event declaration grammar, contract/event IR, bundle hash, reactive module
hash, generated catalog, and versioned durable event record gain explicit
successors. Existing literals and old event bytes remain readable but lack an
anchor and therefore cannot enter a protected consumer. Rotation regenerates
all example locks, modules, generated clients/MCP schemas, protocol fixtures,
and frozen hashes in one receipted change. No old event is guessed into the new
authority model.

The anchor descriptor's inclusion in the durable event-record successor also
makes the authority input available to the replication changelog without a
second inferred representation. On a follower, however, "current row" means
current at that follower's applied state/policy frontier. RE3 must decide and
test whether protected delivery waits for the required frontier, routes to an
authoritative leader, or returns a typed freshness/reset outcome. This ADR
does not permit a follower to evaluate against stale state while claiming
authoritative current-row semantics.

## Security

Default is deny. Payload names and values are never authority. Every delivery
uses current capability revision, current entity state, current indexed
relationship evidence, and the exact compiler-owned policy anchor. Hidden
events do not affect caller-visible items, leases, counts, payload sizes, or
cursor bytes beyond the accepted bounded-progress class.

## Standing Design Tests

- **Interface safety:** an event is either compiler-anchored to one exact row
  policy or unavailable to protected consumers. No request, SDK, MCP caller, or
  event payload can supply an anchor or allow decision.
- **Scale:** evaluation is one partition, one bounded stream window, one row
  plus compiler-bounded indexed relationship probes per candidate, with fixed
  candidate/item/byte/lease ceilings.

Every examined candidate, including a hidden candidate, therefore incurs an
anchored entity read, its compiler-bounded relationship-evidence reads, and a
policy evaluation. That read amplification is an accepted alpha cost. A later
performance campaign may cache or share exact frontier-bound evidence, but it
may not persist allow decisions, skip current-state checks, weaken inference
protection, or expose hidden-candidate cardinality.

## Testing

- Grammar/IR/source-span tests for complete, missing, partial, optional,
  cross-partition, wrong-type, multiple, and unindexed anchors.
- Old/current event-record fixtures and full hash/lock/module/generated-artifact
  rotation receipt.
- Differential delivery tests for owner/group/public, ACL changes, deletion,
  missing rows, revocation, role rotation, seek/replay, lease expiry, and crash.
- Inference tests prove hidden events affect no visible item limit, inflight
  count, notification, result count, event identity, or cursor contents.
- TicketDesk, Payload, MLflow, and Woodpecker contextual/event acceptance under
  the same pure row-policy evaluator.

## Acceptance

Human acceptance of this exact text is required before WP-572 changes event
grammar/IR/durable records or enables protected `ConsumeEventStream` authority.
