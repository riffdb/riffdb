# Contextual agent subscriptions

A contextual subscription turns a durable domain event into bounded agent work
without running an agent, connector, or callback inside the originating
transaction. Its compiled definition binds one exact event stream, up to 16
named hydration queries, up to 32 declared command reactions, canonical
parameters, and fixed delivery limits.

## Work item semantics

One pull leases at most one event. RiffDB executes every hydration in one shared
read snapshot whose application head is at least the event's commit sequence.
The returned item contains:

- the selected typed event and stable event ID;
- attempt-specific lease token, attempt, expiry, and history incarnation;
- one shared `context_head` and bounded named hydration results;
- only reactions that are both declared and currently authorized; and
- an opaque server-sealed causation token for each available reaction.

Hydrated values are never stored in consumer state. Redelivery recomputes them
under current authorization, so an agent must treat each returned item as a
fresh view. Available reactions can still return declared business-rejection
outcomes when command preconditions do not hold.

## Reaction safety

Reaction execution uses the ordinary application command service and commit
coordinator. The contextual permission is not command authority. Before a new
commit, RiffDB reauthorizes the target command and verifies the token's
database, history incarnation, reactive identity, canonical parameter and
consumer identity, event and lease attempt, principal and capability revision,
target command, and expiry against durable lease truth.

RiffDB derives the direct UUID or bounded-string idempotency input from the
reactive module hash, contextual operation hash, event ID, command ID, and
declared reaction name. It replaces caller input at the service boundary. The
command's mutations, outcome, events, causing event, inherited root request,
provenance, and commit record are then atomic.

The reaction operation and its nested command are separate audited service
operations. RiffDB derives a deterministic child request ID for the nested
command so their audit lifecycles cannot collide, while the durable causation
record continues to carry the original root request ID. Applications must not
construct or substitute either identity.

If the reaction commits but its response or the following acknowledgement is
lost, the event is redelivered. Repeating the reaction resolves the persisted
outcome and cannot duplicate authoritative state. An expired token may resolve
that exact prior outcome but cannot authorize a new commit.

## Worker order

1. Pull one contextual work item.
2. Inspect its typed event, fresh hydration, and available reactions.
3. Execute one generated reaction helper or perform separately idempotent work.
4. Persist the successful result or external effect.
5. Acknowledge with the exact work item evidence.
6. Negative-acknowledge with a bounded delay when retry is appropriate.

Never acknowledge before protected work is durable. Never reconstruct a lease
or causation token. On restore, the new history incarnation invalidates old
lease evidence and the durable consumer resumes from its restored checkpoint.

## Public surfaces

The five API-neutral operations are consume, acknowledge, negative
acknowledge, status, and execute reaction. gRPC exposes them as bounded unary
methods. Generated Rust, TypeScript, Python, and MCP artifacts bind the exact
reactive identity. MCP fixed tools use underscore-only names:

- `riffdb_contextual_next`
- `riffdb_contextual_ack`
- `riffdb_contextual_nack`
- `riffdb_contextual_status`
- `riffdb_contextual_react`

MCP uses the same authorization and service path as every other transport. A
resource update is only a coalescible, payload-free wakeup; durable consumer
truth remains the source of correctness.

See [Reactive Modules](MODULES.md), [Reactive Application Clients](CLIENTS.md),
and [MCP for Agents](../mcp/agent-cookbook.md).
