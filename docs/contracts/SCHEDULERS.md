# Bounded Workflow Schedulers

RiffDB's scheduler is an application worker, not a privileged database
subsystem. It discovers eligible work through one exact named RiffQL query or
durable event stream and performs every mutation through generated compiled
commands. It has no storage handle, entity editor, arbitrary query text,
generic transaction callback, or authority derived from a lease.

The POC provides the transport-neutral `riffdb-scheduler` orchestration crate
and generated workflow observations for Rust, Go, TypeScript, and Python. A
deployment still owns the small adapter that maps the closed scheduler port to
its generated application client. There is not yet a standalone scheduler
daemon.

## Least-authority role

A scheduler role contains only its selection operation and the exact commands
for its workflow. For example, the MLflow-shaped acceptance role grants:

```text
query DueRun
command ClaimRun
command CompleteRun
command ReleaseRun
```

The Woodpecker-shaped role consumes the exact `PipelineTransitions` durable
stream and grants only `ClaimPipeline`, `StartPipeline`, and
`ReleasePipeline`. Neither role grants kernel reads, raw field access,
administrative operations, or generic command execution. Server authorization
is rechecked on every operation. A current owner ID and fencing token are
workflow state, never credentials.

## One worker turn

For one bounded work item, the worker performs this sequence:

1. Select at most one item from the configured named query or durable stream.
2. Resolve the stable business-command idempotency identity.
3. Acquire a fresh fenced claim for the current delivery attempt.
4. Execute the exact business command only when no persisted outcome exists.
5. Release the current fence using the generated successor revision.
6. Acknowledge the durable item only after business state and lease cleanup are
   resolved.

Resolving the business outcome before claiming matters after a crash. The
business attempt identity stays stable across redelivery, while claim and
release identities include the nonzero durable delivery-attempt ordinal. A
redelivery therefore cannot replay an obsolete fence, and it cannot execute
the business mutation twice. If the previous process died after the business
commit, the next process acquires and releases a fresh fence before advancing
the checkpoint.

## Attempt identities and bounds

The v1 attempt hash is domain-separated and binds:

- schedule name and exact query or reactive module identity;
- logical due timestamp from application state or the event;
- explicit partition component and bounded target-key suffix; and
- attempt kind, including delivery ordinal for claim and release.

The resulting `sched_<digest>` value is suitable for a generated command's
idempotency input. It contains no plaintext tenant or target data. The current
POC accepts at most eight target components, 1,024 canonical target bytes,
eight in-flight items, ten delivery attempts, a 30-second selection wait, a
5–900 second claim duration, and a one-hour retry-delay cap.

Wakeup hints contain only an opaque generation. They carry no event payload,
cursor, lease, capability, or business key. The worker always obtains the
authorized bounded work item through its named application operation.

## Delivery guarantee

The scheduler composes durable at-least-once delivery with command
idempotency. A process may receive an item again after death, lease expiry,
revocation, cancellation, or a lost acknowledgement. Repeating the stable
business identity resolves the stored command outcome; it does not duplicate
business state or its atomically emitted domain events.

RiffDB does **not** claim exactly-once external effects. External calls belong
after the command commit and should consume durable events or outbox intent
with their own idempotent destination identity.

## Generated workflow evidence

Successful generated workflow commands expose a bounded collection of
symbolic binding names and successor revisions:

```text
WorkflowSuccessorRevision { binding: "run", revision: 42 }
```

Declared failure outcomes expose an empty collection. Callers pass the exact
successor revision to the next fenced command rather than re-reading, guessing
from commit sequence, or silently substituting a newer revision. Rust, Go,
TypeScript, and Python bindings implement the same rule and reject revision
overflow.

The checked fixtures under `fixtures/adapters/mlflow` and
`fixtures/adapters/woodpecker` demonstrate exact role compilation and generated
bindings. `./scripts/adapter-workflow-acceptance --all-languages` checks their
locks and compiles or type-checks all four language surfaces. The
process-level scheduler recovery test exercises both shapes at every selection,
claim, business/event, release, and checkpoint crash boundary, plus duplicate
wakeup, stale worker, revocation, lease expiry, and restart.
