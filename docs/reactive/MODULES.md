# Reactive modules

Reactive module grammar V1 is the compile-time application surface for named
event streams, query watches, and contextual subscriptions. Authors place the
bounded source in a `.riffr` file and declare it in Application Source V4.
RiffDB resolves every name against one exact contract and immutable query
modules before it creates any deployable artifact.

```riff
reactive TicketActivity version 1 {
  stream TicketEvents($organization_id: Ticket.organization_id) {
    partition (organization_id = $organization_id);
    event TicketChanged select (organization_id, ticket_id, status);
  }
  watch TicketWatch(
    $organization_id: Ticket.organization_id,
    $ticket_id: Ticket.ticket_id
  ) query GetTicket updates patch;
  subscription TicketAgent($organization_id: Ticket.organization_id) {
    stream TicketEvents(organization_id = $organization_id);
    hydrate ticket query GetTicket(
      organization_id = $organization_id,
      ticket_id = event.ticket_id
    );
    reaction assign command AssignTicket;
    limits { batch 4; in_flight 4; lease_seconds 60; }
  }
}
```

## Static guarantees

- A stream has one complete partition binding shared by every selected event.
- Event types and payload fields are explicit. Predicates can use only selected
  fields and typed parameters.
- A patch watch is accepted only when its query returns a complete authorized
  primary key. Other bounded queries use reset updates.
- A subscription binds one exact stream, no more than 16 bounded hydration
  queries, no more than 32 named command reactions, and fixed delivery limits.
- Aggregate query work remains within 500 rows and encoded output remains
  within four MiB.
- Batch and in-flight limits are each 1 through 8. Leases are 5 through 900
  seconds.

Reactive source, each compiled operation, and the complete module have separate
domain-separated hashes. Lock V5 records the grammar and IR versions, all
source/module/operation hashes, exact query dependencies, V2 role definitions,
and generated artifact hashes.

## Least-authority roles

Application Source V4 roles name `event_streams`, `watch_queries`, and
`agent_subscriptions`. The compiler lowers those names to exact permissions
bound to contract lineage, reactive module hash, and operation name:

| Role declaration | Derived permission |
|---|---|
| `event_streams` | `ConsumeEventStream` |
| `watch_queries` | `WatchNamedQuery` |
| `agent_subscriptions` | `ConsumeContextualSubscription` |

`SeekEventStreamConsumer` is separate administrative authority. Normal role
compilation never grants it as a consequence of consuming a stream. Reactive
permissions also do not imply raw commit-log, entity, index, or ad-hoc query
access.

## Current delivery boundary

WP-416 provides the grammar, compiler, exact application artifacts, and role
permissions. Durable consumer execution is added by WP-417, live query delivery
by WP-418, generated client and MCP surfaces by WP-419, and contextual work-item
execution by WP-420. A V4 application can be checked and locked now; those later
packages own the corresponding runtime operations.

See [Domain Events](../contracts/DOMAIN-EVENTS.md), [Immutable Query
Modules](../riffql/MODULES.md), and [Application Source and Exact
Lock](../getting-started/APPLICATION-MANIFEST.md).
