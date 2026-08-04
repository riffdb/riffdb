# Reactive application clients

Application Source V4 generates reactive operations for Rust, TypeScript,
Python, and MCP from the same exact reactive-module identity. The CLI exposes
the same API-neutral service operations. Generated code owns operation names,
module hashes, parameter types, event variants, and live-query result variants;
application code supplies only values, a durable consumer name, and bounded
delivery options.

Regenerate after reviewing the source and exact lock:

```bash
riffdb application generate --locked
riffdb application lock --check
```

Do not hand-copy a module hash or recreate an event or live-update union in
application code. A changed generated reactive diff is an application API
change and must be reviewed with the source and lock that produced it.

## Durable event consumers

A consumer identity includes the selected database, immutable reactive-module
hash, operation name, canonical parameters, and consumer name. Reuse the same
consumer name and parameters to resume its checkpoint. Changing any identity
component selects a different consumer; it does not rename or inherit the old
one.

Delivery is at least once and partition ordered. For each delivery:

1. Validate and handle the generated closed event variant.
2. Commit any external side effect or idempotent RiffDB reaction.
3. Acknowledge with the exact delivery object or its retained lease evidence.
4. Negative-acknowledge with a bounded retry delay when work should be retried.

An event ID is stable, but its lease token is attempt-specific. A stale,
expired, or already-resolved lease returns a closed mutation result and never
grants authority by possession. Do not acknowledge before the work it protects
is durable. Seek is a separately authorized administrative operation and is not
implicitly granted to a normal consumer role.

The generated Rust client exposes a typed consumer, closed event enum, typed
delivery batch, `next_<stream>`, acknowledge, negative-acknowledge, seek, and
status methods. The stable facade also exposes a checked streaming response for
workers that keep one gRPC stream open. TypeScript and Python expose generated
async iterators and operation-specific acknowledgement helpers; reconnecting
the iterator uses the same durable consumer identity.

## Contextual agent subscriptions

Generated contextual clients expose one operation-specific `next`, ack, nack,
status, and reaction helper. A pull returns zero or one work item. The item
contains the typed event, one authoritative `context_head`, bounded named-query
hydrations, exact lease evidence, and only the declared reactions currently
authorized for the caller.

Pass the returned work item to generated reaction and acknowledgement methods;
do not reconstruct its token fields. RiffDB derives the reaction command's
idempotency input from the immutable subscription identity, event ID, command,
and reaction name. Caller-provided idempotency input is replaced before command
validation. A retry after an uncertain response therefore resolves the same
outcome. Acknowledge only after the reaction outcome or intended external work
is durable.

Context is recomputed on redelivery from one shared snapshot at or beyond the
event sequence. It is not persisted in the consumer record. Available means
the command is authorized at delivery time, not that its business preconditions
will succeed. Reaction execution performs fresh authorization and validates the
sealed token, live lease, restore incarnation, database, partition, principal,
and target command.

## Live named queries

Generated watch methods return the closed `Snapshot`, `Patch`, `Reset`,
`Checkpoint`, and `Terminal` update vocabulary. Apply updates in delivery order:

- Replace local state with a `Snapshot` or `Reset` result.
- Apply every operation in one `Patch` atomically to the named result field.
- Persist the cursor after the corresponding state update is durable.
- Advance only cursor and frontier on `Checkpoint`.
- Clear all retained result state and stop on `Terminal`.

Cursors are opaque evidence, not authority. Rust persists their bytes without
interpreting them. TypeScript, Python, CLI, and MCP use standard padded Base64.
Reconnect with the last applied cursor and the same exact operation and
parameters. RiffDB reauthorizes the watch and returns a complete snapshot or a
typed reset when continuation evidence is no longer valid.

The TypeScript generator also emits a framework-neutral live store. Its
generated SSE relay accepts an application-owned `authorized()` callback and
an already-authorized update iterator. Run that relay in the application
server. Browser JavaScript receives application-authenticated SSE data but
never a RiffDB endpoint credential, capability, lease token, or direct database
connection.

## CLI operations

Use the exact module hash from the application lock:

```bash
riffdb event consume \
  --module-hash <64-hex-module-hash> \
  --operation TicketEvents \
  --parameter 'organization_id={"type":"uuid","value":"018f6f50-6f31-7d62-9a7e-4f8b913d2f11"}' \
  --consumer-name ticket-worker \
  --batch-limit 4 \
  --lease-seconds 60

riffdb event status \
  --module-hash <64-hex-module-hash> \
  --operation TicketEvents \
  --parameter 'organization_id={"type":"uuid","value":"018f6f50-6f31-7d62-9a7e-4f8b913d2f11"}' \
  --consumer-name ticket-worker

riffdb query watch \
  --module-hash <64-hex-module-hash> \
  --parameter 'organization_id={"type":"uuid","value":"018f6f50-6f31-7d62-9a7e-4f8b913d2f11"}' \
  TicketWatch
```

`event ack`, `event nack`, and `event seek` use the exact identity printed by
the consume or status result. `query watch` returns one update and cursor per
invocation so shell programs can durably apply the update before reconnecting.
See the [CLI reference](../reference/CLI.md) for every bound and option.

Contextual workers use the same generated module, operation, parameters, and
consumer identity:

```bash
riffdb contextual next \
  --module-hash <64-hex-module-hash> \
  --operation TriageTicket \
  --parameter 'organization_id={"type":"uuid","value":"018f6f50-6f31-7d62-9a7e-4f8b913d2f11"}' \
  --consumer-name triage-worker \
  --wait-nanos 30000000000

riffdb contextual react \
  --module-hash <64-hex-module-hash> \
  --operation TriageTicket \
  --parameter 'organization_id={"type":"uuid","value":"018f6f50-6f31-7d62-9a7e-4f8b913d2f11"}' \
  --consumer-name triage-worker \
  --reaction comment \
  --causation-token <token-from-next> \
  --command-name CreateComment \
  --input comment.json \
  --expected-version 1
```

`contextual ack` and `contextual nack` require the exact event ID, lease token,
and history incarnation returned by `contextual next`; `contextual status`
uses only the stable consumer identity. Prefer the generated SDK helpers in
application workers because they carry this evidence without reconstructing
it. The raw CLI is intended for process integrations and diagnosis, and still
uses the same authorization and application-service path as generated clients.

## MCP operations and wakeups

MCP exposes the fixed underscore-only tools `riffdb_event_next`,
`riffdb_event_ack`, `riffdb_event_nack`, `riffdb_event_seek`,
`riffdb_event_status`, and `riffdb_query_watch`. Generated application MCP
artifacts additionally contain exact operation-specific underscore-only names
and JSON Schemas. Every call resolves current authorization and uses the same
application service as gRPC, CLI, and the SDKs.

Contextual roles additionally expose `riffdb_contextual_next`,
`riffdb_contextual_ack`, `riffdb_contextual_nack`,
`riffdb_contextual_status`, and `riffdb_contextual_react`, plus generated
operation-specific tools. The reaction call forwards the causation token from
the work item; possession of that token never bypasses current policy or lease
validation.

An MCP `notifications/resources/updated` message is only a payload-free wakeup
hint. Its parameters contain one authorized resource URI and no event,
parameters, context, field values, or lease evidence. The client must retrieve
work with `riffdb_event_next` or refresh a watch with `riffdb_query_watch`;
reading a notification is never an acknowledgement. Notifications may
coalesce, so correctness must depend on the durable consumer checkpoint or live
cursor rather than notification count.

See [Reactive Modules](MODULES.md), [Live Named Queries](LIVE-QUERIES.md), and
[MCP for Agents](../mcp/agent-cookbook.md).
