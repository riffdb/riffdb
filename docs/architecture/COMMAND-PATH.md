# Command Execution Path

The command path separates untrusted adaptation, deterministic evaluation, and
authoritative commit.

| Stage | Primary owner | Output |
|---|---|---|
| Decode and bound | Transport adapter | Structurally checked request |
| Select database and authenticate | Shared service and auth | Principal bound to one audience |
| Resolve schema and authorize | Catalog, service, policy | Exact executable operation |
| Recover idempotency | Idempotency/service | Stored outcome or new attempt |
| Acquire logical conflicts | Conflict manager | Cancellation-safe capability guard |
| Evaluate | Deterministic runtime | `CommitIntent` |
| Revalidate and commit | Commit coordinator | `CommittedOutcome` |
| Persist | Storage engine | Atomic authoritative command graph |
| Publish derived work | Server-owned sinks | Outbox/projection notification |

## Boundaries that must remain closed

- Transports cannot construct an already-authorized command.
- Runtime code cannot assign a commit sequence or write storage.
- Storage cannot call arbitrary application logic inside its transaction.
- Conflict ownership cannot substitute for exact read and predicate
  revalidation.
- MCP cannot obtain broader behavior than the same credential has over the
  shared service.
- Projection state cannot become authoritative through a read shortcut.

See [Command Lifecycle](../concepts/COMMAND-LIFECYCLE.md) for caller-visible
behavior and the [System Overview](OVERVIEW.md) for the process layout.
