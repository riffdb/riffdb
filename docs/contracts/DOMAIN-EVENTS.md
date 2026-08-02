# Domain events

Application-facing consumption is defined with bounded `.riffr` reactive
modules, not raw commit-log access. See [Reactive Modules](../reactive/MODULES.md)
for partition proofs, selected payload fields, exact identities, and role
permissions.

RiffDB domain events are immutable facts committed atomically with the command's
state changes, declared outcome, idempotency result, provenance, and commit
record. They are not a second event store and do not require reconstructing
entity state from events.

## Declaring an application-streamable event

An event is available to symbolic replay only when it declares the ordered
payload fields that derive its application partition:

```riff
event TicketCreated {
  partition_by (organization_id)

  organization_id: uuid
  ticket_id: uuid
  created_at: timestamp
}
```

Every command that emits `TicketCreated` must supply the exact command partition
expression under the same aggregate-namespaced canonical key schema. The
compiler rejects a merely equal-looking value, a different expression, a
different aggregate namespace, a missing field, or a cross-partition emit.
There is no runtime fallback to a global scan.

Events without `partition_by` remain valid sources for projections and outbox
delivery, but the application event catalog marks them as unavailable for
replay and streaming.

## Evolution

Events use the contract version and stable event symbol as their only evolution
clock. Adding an optional payload field is compatible and historical events are
materialized with `null` by the catalog. Changing a field's meaning requires a
new event symbol. Once an event has a partition proof, removing or changing that
proof is incompatible.

Adding a partition declaration to a previously unpartitioned event is allowed
only when the compiler proves every retained writer shape. Historical routing
is rebuilt from authoritative commits, and every replayed payload must derive
the same partition as its enclosing commit before it can cross the application
boundary.

## Replay safety

Application replay is symbolic and single-partition. Callers select an event
name, exact partition fields, and an explicit bounded payload field set. RiffDB
then joins each route to the immutable event, enclosing commit, historical
writer plan, and active lineage materialization proof. Missing or contradictory
evidence fails the complete page.

The normal envelope may include:

- stable event identity and symbolic event name;
- writer contract version and plan identity;
- commit sequence/ordinal and logical occurrence time;
- symbolic command name and command request correlation;
- safe actor kind and opaque provenance locator;
- history incarnation, opaque cursor, and explicitly selected payload fields.

It never includes raw principal or agent-session identity, partition or
conflict keys, unselected payload, credentials, or process-local trace data.

Replay order is increasing `EventId` within exactly one logical partition. This
does not promise global order, time-based retention, raw CDC, exactly-once
delivery, or event-sourced reconstruction.

## Inspecting events

WP-415 provides an operator-oriented event catalog over the shared application
service. `event describe` requires `ReadContract`; `event replay` and `event
tail` require `ReadCommit`. These permissions are intentionally broader than
the least-authority named-stream permissions generated for applications in the
next phase.

Describe the active declaration before constructing a replay request:

```bash
riffdb --database ticketdesk event describe TicketCreated
```

Replay requires every ordered partition component and at least one explicitly
selected payload field. CLI partition values use the same tagged canonical JSON
shape as other kernel commands:

```bash
riffdb --database ticketdesk event replay TicketCreated \
  --partition 'organization_id={"type":"uuid","value":"018f6f50-6f31-7d62-9a7e-4f8b913d2f11"}' \
  --field ticket_id \
  --field created_at \
  --limit 50
```

`next_cursor` is opaque, process-local, principal-bound, and valid for five
minutes. It may be present on an empty page because the route scan advanced
past other event types in the same partition. Resume such a page with
`--cursor`; do not infer completion from an empty `items` array alone.

Tail performs a race-free catch-up before waiting for a commit wakeup and then
replays authoritative routes again. It waits for at most 30 seconds and reports
`wait_timed_out: true` only when that interval expires without a selected event:

```bash
riffdb --database ticketdesk event tail TicketCreated \
  --partition 'organization_id={"type":"uuid","value":"018f6f50-6f31-7d62-9a7e-4f8b913d2f11"}' \
  --field ticket_id \
  --after 41:0 \
  --wait-nanos 30000000000
```

Both replay and tail return the current `history_incarnation`. Supplying it on
a later call detects restore and fails closed instead of interpreting a stale
position against replacement history. A tail request does not accept a replay
cursor; use the last returned event ID as its next `--after` position.
