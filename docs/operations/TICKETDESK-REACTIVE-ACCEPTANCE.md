# TicketDesk reactive acceptance

TicketDesk is the P8 exit application. Its checked Application Source V4 and
Lock V5 live under `examples/ticketdesk/`. The lock binds one exact TicketDesk
contract, 13 named queries, one `TicketActivity` reactive module, two symbolic
application roles plus a seed-only role, and generated Rust, TypeScript,
Python, and MCP artifacts.

## Accepted workflow

`CreateTicket` atomically emits the authoritative `TicketCreated` event in the
same commit as state, outcome, idempotency, provenance, and commit evidence.
The event declares `organization_id` as its application partition.

`TicketQueueWatch` watches the bounded, keyed `TicketQueue` result and may emit
patches. `TicketPageWatch` watches the complete bounded `TicketPage` and emits
resets when its multi-result view changes. A browser connects only to the
application-owned authenticated SSE relay in
`examples/ticketdesk/web/src/server.ts`; no RiffDB capability, lease, or
causation token enters browser code.

`TriageTicket` leases `TicketCreated`, hydrates `TicketPage` in one shared
snapshot at or beyond the event commit, and exposes `CreateComment` only when
currently authorized. The generated `react_comment` helpers replace the
caller idempotency input with the retry-stable causal identity. A worker
acknowledges only after the reaction result is durable.

`TicketDeskSeeder` is a development-only role restricted to
`CreateOrganization`, `CreateUser`, and `CreateProject`. It has no query,
watch, stream, contextual, or ticket-mutation authority and is never used by
the browser relay or agent worker.

## Verification

Run the P8 gate tests:

```bash
cargo test -p riffdb-testkit --test durable_event_consumers --all-features
cargo test -p riffdb-testkit --test live_named_queries --all-features
cargo test -p riffdb-testkit --test contextual_agent_subscriptions --all-features
cargo test -p riffdb-testkit --test reactive_ticketdesk_acceptance --all-features
./scripts/check-application-bindings
```

The TypeScript acceptance creates two independent queue stores and one detail
store, sends updates through the generated relay, verifies both queue views
converge, and verifies authorization termination clears every retained view.
The Rust gate recompiles the source and lock, checks partition and operation
plans, audits generated disclosure, checks least-authority roles, and proves
compatible event evolution and definition identity change. Lower-level suites
supply deterministic snapshot/catch-up, saturation, restore-incarnation,
multi-database, durable consumer, and commit-before-ack evidence.

The independent implementation gate is distinct from those repository-owned
tests. `scripts/reactive-ticketdesk-evaluation-package` creates a sealed public
bundle containing symbolic TicketDesk author sources but no generated bindings,
relay, worker, test implementation, RiffDB source, or repository TicketDesk
implementation. One fresh evaluator must generate and complete the browser,
contextual reaction, recovery, authorization, MCP disclosure, and boundary
checks. Published evidence is verified with:

```bash
TMPDIR="$HOME/tmp" ./scripts/reactive-ticketdesk-evaluation-acceptance \
  --campaign wp421-reactive-01 --assert-gate
```

## Recovery and evolution

A reconnect supplies the last applied opaque cursor under the same exact watch
identity. Restore, authorization drift, or definition drift produces a typed
reset or terminal result. Generated stores clear protected state on terminal.

Changing reactive source uses a new immutable module identity; a consumer does
not inherit a checkpoint from the predecessor definition. Adding an optional
event field uses the originating contract version and catalog normalization;
changed meaning uses a new event type. Restore advances history incarnation and
invalidates pre-restore cursors and leases.

## Deliberate limits

P8 does not provide raw CDC, global or cross-partition order, physical
time-based event retention, exactly-once external effects, event-sourced
reconstruction, persisted hydrated context, direct browser credentials,
webhooks, connectors, arbitrary callbacks, or in-process agent inference.
