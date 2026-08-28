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

An `init_or_mutate` binding does not add a stage or a second command. Snapshot
materialization records one exact absence or presence observation. Deterministic
evaluation constructs one provisional initialized record or loads one exact
preimage, applies the common compiled suffix, and emits one create or one
revision-checked replace. The coordinator revalidates that original observation
before committing the same atomic outcome, event, provenance, and mutation
graph. It never converts a stale create candidate into a replace (or the
reverse) without whole-command reevaluation.

An `observe_or_initialize`/`decide` pair uses the same stages. Snapshot
materialization retains the exact absent or present-version observation and
the compiler-enumerated finite target set. Deterministic evaluation tests the
ordered predicates once for that decision instance and constructs only the
selected apply graph, exact no-effect dependency, or typed rejection. Worker
preparation replays the sealed V18 program once against the normalized
snapshot, caches its mutation coverage, and the writer revalidates the exact
observations before accepting that proof. Rejection discards the whole graph;
all-no-effect still commits the normal outcome, audit, provenance, and
sequence without an application mutation.

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
