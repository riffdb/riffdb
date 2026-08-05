# RiffDB MCP Agent Cookbook

RiffDB exposes policy-filtered database operations through MCP. Use
`tools/list` after initialization to discover the commands authorized for the
current credential and database. Tool names contain lowercase letters, digits,
and underscores only.

## Values

- Send signed or unsigned integers as ordinary JSON integers when the generated
  schema says `integer`; RiffDB resolves signedness after selecting the
  operation schema. Tagged `{"$i64":"2"}` and `{"$u64":"2"}` remain accepted
  compatibility forms.
- Send fixed-scale decimals with the generated exact decimal object. Do not
  send a JSON floating-point number.
- Send UUID values as canonical lowercase strings, for example
  `"01900000-0000-7000-8000-000000000001"`.
- Send enum variants as their declared strings, for example `"Linear"`.
- Follow each tool's `inputSchema`; undeclared properties are rejected.

## Mutating Commands

Choose a fresh caller-owned `idempotency_key` for each logical command. Preserve
that key until the result is known. Never generate a new key merely because a
connection was interrupted.

A declared business rejection is a successful MCP tool call with a typed
outcome. Branch on its declared outcome field rather than parsing text or
treating it as a protocol failure.

If a connection fails after submission, the command may have committed. Retry
the same command with the same input and idempotency key, or resolve the
returned outcome URI. Reusing a key with different canonical input fails
closed.

## Discovery and Documentation

`tools/list` is the authorized catalog for the selected database and current
credential. A missing command may be absent, stale, or unauthorized; do not
infer which.

For a command resource:

- `riffdb://command/<lineage>/<command-id>/docs` is the invocation guide with
  inputs, a complete MCP call example, declared outcomes, and retry guidance.
- `riffdb://command/<lineage>/<command-id>/plan` is the compiler-oriented
  executable plan and generated schema detail.

Use `/docs` to call a command. Use `/plan` when inspecting exact compiler or
dependency behavior.

## Input Errors

Invalid tool arguments return one redacted
`riffdb.mcp.input-error/v1` diagnostic. Its `path` is a JSON Pointer, `code`
classifies the first deterministic violation, and `expected` describes public
schema shape. The error never echoes the submitted value.

## Database Selection

Each MCP connection is bound to one configured database alias. Stdio selects it
in `mcp.toml`; hosted MCP uses the `riffdb-database` header. Credentials are
database-bound and cannot authorize another database.

Authenticated `server_health` and active-contract responses include the
selected `database` alias; health also includes the configured authentication
`audience`. Check these before deploying or mutating. To validate an stdio
configuration without starting an MCP session, run:

```bash
riffdb-mcp doctor --config ~/.config/riffdb/mcp-ea.toml
```

`doctor` first uses authenticated health when the role permits it. A narrow
application role need not gain health authority: the generated application MCP
config retains the expected audience, and `doctor` instead proves the
credential and database route through capability-filtered tool discovery.

The bootstrap MCP credential is an authoring identity: validation, catalog,
deployment, and health. A separately bound application-role credential is the
runtime identity for named queries and commands. Register a second MCP process
with that application credential rather than widening the bootstrap identity.

Application-tool discovery is deliberately not an execution proof. It shows a
compiled command only for an exact `InvokeCommand` permission and a named query
only for an exact contract-lineage, query-module-hash, and query-name
`ExecuteNamedQuery` permission. Tenant and partition scoping do not hide the
operation because its actual partition is derived from invocation inputs. Each
call resolves the tool again and performs fresh current-policy authorization
against those inputs; revocation, scope, module substitution, and query-name
substitution therefore fail closed after an earlier successful tool listing.

Deployed named-query tools use the compiler-owned
`<module_snake>_<query_snake>` name, advertise the generated parameter and
result schemas, and execute one named query in one snapshot. They never submit
ad-hoc RiffQL or gain raw entity/index authority.

## Reactive operations

Reactive application roles may discover `riffdb_event_next`,
`riffdb_event_ack`, `riffdb_event_nack`, `riffdb_event_seek`,
`riffdb_event_status`, and `riffdb_query_watch`, filtered by their exact
permissions. Tool names contain underscores rather than dots for broad MCP host
compatibility.

Reactive parameters use the exact tagged scalar objects advertised by each
tool schema. This prevents JSON number precision loss and preserves durable
type identity. For example, UUID and signed-integer parameters are written as:

```json
{
  "parameters": {
    "workspace_id": {
      "type": "uuid",
      "value": "01900000-0000-7000-8000-000000000001"
    },
    "after_sequence": { "type": "i64", "value": "42" }
  },
  "consumer_name": "Worker_1"
}
```

Decimals include their fixed `precision` and `scale`; enums include the
compiler-pinned `type_id`, `variant_id`, and display `name`. Use the generated
schema rather than guessing these identities.

Treat event delivery as at least once. Perform the intended idempotent work,
then acknowledge with the exact event ID, lease token, and history incarnation
returned by `riffdb_event_next`. A lease token is not authority and may be stale
after retry, restore, or expiry. Seek requires separate administrative
authority.

Persist the cursor from each applied `riffdb_query_watch` update. Snapshot and
reset replace local state; patch applies its ordered closed operations;
checkpoint advances the cursor without changing state; terminal requires the
agent or application to clear retained query data and stop.

Subscribe to `riffdb://reactive/wakeup` for reactive work hints. Its resource
body contains one opaque generation and `notifications/resources/updated`
contains only that URI. Both are coalescible hints, never an event delivery,
work count, commit sequence, or acknowledgement. Retrieve work through an
authorized reactive tool and rely on the durable checkpoint or cursor for
correctness. Fetch once immediately after subscribing; the first notification
represents a later change, not the current queue state. See
[Reactive Application Clients](../reactive/CLIENTS.md).

## Contextual work and reactions

Use `riffdb_contextual_next` for an agent subscription. One call returns at
most one leased event, fresh named-query hydrations sharing one
`context_head`, and the currently authorized declared reactions. Use only a
reaction returned in that item.

Call `riffdb_contextual_react` with the exact reaction name and causation token
from the item. RiffDB derives and binds the command idempotency value, validates
the live lease and current authority, and records the causing event with the
command provenance. If the response is uncertain, repeat the same reaction;
do not invent a replacement command identity. An expired token can resolve an
already committed exact reaction but cannot admit a new command.

After durable success, call `riffdb_contextual_ack` with the event ID, lease
token, and history incarnation. Use `riffdb_contextual_nack` for a bounded
retry and `riffdb_contextual_status` to inspect durable progress. A redelivered
item has freshly recomputed context and may expose a different authorized
reaction set. MCP notifications remain payload-free hints; they never consume
or acknowledge contextual work.
