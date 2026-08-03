# Live named queries

A live named query watches one exact `watch` operation from a published
reactive module. It is not an ad-hoc query, a raw commit subscription, or a
storage change feed. The watch resolves an immutable query module and plan,
executes that bounded RiffQL query in one authoritative snapshot, and then uses
authoritative commits only as invalidation signals.

## Define a watch

The query must already be part of an immutable query module. A reactive module
binds its parameters and update mode:

```riff
reactive TicketActivity version 1 {
  watch OpenTickets(
    $organization_id: Ticket.organization_id,
    $project_id: Project.project_id
  ) query OpenTicketsPage updates patch;
}
```

Use `updates patch` only when the query has one top-level `many` result and
explicitly selects every primary-key field. Otherwise use `updates reset`.
Paginated queries with an `after` cursor are not watchable because they expose
only a partial result.

The caller needs the exact `WatchNamedQuery` permission for the contract
lineage, reactive module hash, and operation name. Query parameters and field
visibility remain subject to the same application-query policy used by normal
named RiffQL execution. A cursor does not grant authority.

## Update protocol

`QueryService.WatchNamedQuery` is a server-streaming gRPC operation. Every
stream starts with one of these complete views:

- `Snapshot`: a fresh authorized result, frontier, and resume cursor.
- `Reset`: a fresh complete result when supplied cursor evidence cannot be
  continued, with a typed reason.

Later messages use the closed update vocabulary:

- `Patch`: ordered insert, remove, replace, or move operations for one keyed
  top-level collection.
- `Reset`: a complete result after an outcome change or a diff that exceeds
  the patch bound.
- `Checkpoint`: the result is unchanged, but the authoritative frontier and
  cursor advanced.
- `Terminal`: the watch is closed and carries no result payload.

A patch key contains only the complete explicitly selected public key fields.
Internal entity keys, stable field IDs, commit change records, and unselected
fields never cross the public boundary.

## Frontiers and reconnect

The initial query reads at application head `S`. RiffDB then subscribes to
authoritative commits strictly after `S`; durable catch-up closes the race
between snapshot completion and subscription registration. Notifications are
wakeup hints. Every relevant change is re-read through authoritative storage
and the bounded query is re-executed.

Retain the cursor from the last applied update. On reconnect, submit the same
reactive module hash, operation name, canonical parameters, and that cursor.
RiffDB freshly authenticates and authorizes the request and returns a complete
snapshot or typed reset. A cursor binds:

- history incarnation;
- exact contract, reactive operation, query module, and query plan;
- canonical parameter hash and partition set;
- application frontier and expiry.

Restore, expiry, or identity drift therefore cannot become an ambiguous
continuation. Reconnecting from an older delivered cursor is safe because the
new watch executes a fresh authoritative snapshot.

## Limits and closure

One database admits at most 128 live watches. A watch lives for at most 15
minutes, retains no more than the bounded stream state, emits at most 500 patch
operations in four MiB, and uses cursors no larger than 4,096 bytes. Burst
processing may skip transient intermediate views, but the delivered state must
converge to a fresh execution of the named query.

Typed terminal reasons include authorization change, buffer pressure, lifetime
expiry, service unavailability, integrity failure, and definition change.
Authorization is revalidated before delivery. Closing for revocation discards
the service-owned retained result before releasing the subscription.

## Rust transport client

The current low-level Rust transport accepts a checked
`riffdb_proto::v1::WatchNamedQueryRequest` through
`RiffDbClient::watch_named_query`. Read messages with
`LiveQueryUpdateStream::message`; strict public decoding rejects unknown,
missing, oversized, noncanonical, or internally identified fields.

Generated application-specific Rust, TypeScript, Python, CLI, MCP, and browser
relay surfaces adapt this same checked service boundary. Every generated client
persists the last applied opaque cursor for reconnect and clears retained
result state when `Terminal` is received. See [Reactive Application
Clients](CLIENTS.md) for the operation-specific surfaces and relay boundary.

See [Reactive Modules](MODULES.md), [RiffQL Planning](../riffql/PLANNING.md),
and [Consistency and Recovery](../concepts/CONSISTENCY.md).
