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

## Publication and durable consumption

`riffdb application deploy` publishes every locked reactive module after its
exact contract and immutable query modules are available. Publication compiles
the reviewed `.riffr` source on the server, verifies its name, version, module
hash, contract identity, and query dependencies against the application lock,
then retains the module atomically through the shared authorized control plane.
Publishing the same immutable module again is idempotent. Reusing a name and
version for different content is a closed version conflict.

A durable consumer is identified by the complete tuple of reactive module hash,
operation name, canonical typed parameters, and consumer name. It is therefore
not silently rebound when a module changes. Within one consumer, eligible
events are leased in partition order with at-least-once delivery. An event is
advanced only by an exact acknowledgement carrying the event ID, lease token,
and current history incarnation. Negative acknowledgement schedules a bounded
retry; lease expiry does the same during recovery. The tenth failed attempt is
retained as a dead letter so later eligible events can continue without
pretending the failed event was acknowledged.

Use the exact reactive module hash from the application lock or deployment
state. Parameters use `NAME=JSON_VALUE`, so string values include JSON quotes:

```bash
riffdb event consume \
  --module-hash <64-hex-module-hash> \
  --operation TicketEvents \
  --parameter 'organization_id="acme"' \
  --consumer-name ticket-indexer \
  --batch-limit 4 \
  --lease-seconds 60

riffdb event ack \
  --module-hash <64-hex-module-hash> \
  --operation TicketEvents \
  --parameter 'organization_id="acme"' \
  --consumer-name ticket-indexer \
  --event-id <commit-sequence:event-ordinal> \
  --lease-token <64-hex-lease-token> \
  --history-incarnation <incarnation>

riffdb event status \
  --module-hash <64-hex-module-hash> \
  --operation TicketEvents \
  --parameter 'organization_id="acme"' \
  --consumer-name ticket-indexer
```

`event nack` accepts the same lease identity plus an optional retry delay.
`event seek` is an administrative checkpoint move and requires separately
granted seek authority. `event retire` permanently retires that exact consumer
and releases its retention fence. An active consumer's checkpoint prevents
history pruning past the commit that still contains its first required event.

Backup and restore preserve consumer state but publish a new history
incarnation. Outstanding pre-restore lease tokens then fail closed, and the
consumer resumes from its restored checkpoint. Consumers in sibling named
databases remain isolated.

## Delivery boundary

The public surface includes immutable publication, durable pull and gRPC
stream consumption, leases, acknowledgement, retry, dead-letter, recovery,
seek, retire, status, and retention fencing. Live queries use compiler-derived
invalidation, checked cursor reconnect, and the update protocol described in
[Live Named Queries](LIVE-QUERIES.md). Generated Rust, TypeScript, Python, MCP,
CLI, and application-owned browser relay helpers adapt those same operations as
described in [Reactive Application Clients](CLIENTS.md).

Contextual subscriptions combine one leased event with freshly authorized,
same-snapshot hydration and currently authorized declared reactions. A normal
event-consumer permission does not imply contextual, query, or command
authority. MCP uses the same service boundary and its resource notifications
contain no event or context payload. See [Contextual Agent
Subscriptions](CONTEXTUAL-SUBSCRIPTIONS.md).

See [Domain Events](../contracts/DOMAIN-EVENTS.md), [Immutable Query
Modules](../riffql/MODULES.md), and [Application Source and Exact
Lock](../getting-started/APPLICATION-MANIFEST.md).
