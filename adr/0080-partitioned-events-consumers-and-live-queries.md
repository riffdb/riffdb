# ADR-0080: Partitioned Events, Durable Consumers, and Live Queries

- **Status:** Accepted
- **Date:** 2026-07-31
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `EVT-001` through `EVT-008`, `CON-001` through
  `CON-008`, `LIVE-001` through `LIVE-008`, and `CTX-001` through `CTX-008`
- **Related work packages:** `WP-414` through `WP-421`
- **Amends:** ADR-0013, ADR-0017, ADR-0049, ADR-0055, ADR-0056, ADR-0057,
  ADR-0063, ADR-0064, ADR-0072, and ADR-0075
- **WP-415 public-inspection clarification accepted:** 2026-08-02
- **WP-416 reactive-module clarification accepted:** 2026-08-02
- **WP-417 durable-consumer clarification accepted:** 2026-08-02

## Context

RiffDB already commits typed durable events atomically with command state,
outcomes, idempotency, provenance, and commit records. Projection workers and
the outbox consume those events, and the public kernel exposes a bounded
administrative commit subscription. None of those surfaces is yet the normal
application interface for reliably reacting to named business events or
watching a named RiffQL result.

Exposing the commit log or raw row changes would recreate the storage-shaped
application boundary that RiffQL removed. Adding an independent broker would
also split event identity, authorization, recovery, and causality from the
command transaction that produced the fact. Live queries additionally require
a race-free transition from a consistent query snapshot to later changes.

## Decision

### Existing events remain authoritative

`StoredDurableEventV1`, its `EventId`, canonical payload, and event hash remain
the authoritative event fact. Application event envelopes are catalog- and
service-materialized views joined to the enclosing immutable commit and
provenance. The event row is not rewritten or duplicated to add presentation
metadata.

Event declarations gain an optional ordered `partition_by` field tuple. An
event without that clause remains valid for projections and outbox delivery but
is not application-streamable. For a streamable event, the compiler must prove
that every emit construction supplies the exact command partition expressions
under the same canonical key schema. An unproved or cross-partition emit is a
compile failure, never a runtime fallback.

Events retain one evolution clock: the contract lineage/version and stable
`EventTypeId`. There is no independent event version. Compatible optional-field
evolution remains catalog-normalized; a semantic meaning change requires a new
event type.

A generic event-route index ordered by `(partition hash, EventId)` is rebuilt
from retained commits and maintained atomically with future event commits. It
is an integrity-checked routing index over authoritative facts, not another
event payload owner. Historical events first interpreted under a later
partition declaration are delivered only after catalog materialization derives
the partition from the payload and proves equality with the enclosing commit.

### Reactive application identity

Application Source V4 and Lock V5 add exact reactive modules after the
migration-owned Source V3 and Lock V4 formats. Earlier formats remain
byte-for-byte compatible. A reactive module contains named event streams and
contextual subscriptions. Exact lock identity covers canonical source, event
and query dependencies, plans, result schemas, bounds, generated artifacts,
and derived role authority.

Roles name event streams, watched queries, and contextual subscriptions
explicitly. Permission to execute a named query once does not imply permission
to watch it. Permission to consume a contextual subscription does not grant
standalone access to its hydration queries.

One stream selects explicit event types and explicit payload fields, covers
one concrete partition through typed parameters, and has only bounded typed
predicates. All selected event types have one compatible declared partition
schema. Cross-partition streams are rejected in this phase.

### Durable consumers

Consumer identity is the exact database, reactive module hash, operation name,
canonical parameter hash, and bounded consumer name. Consumers are durable
operational metadata, not application entity state; their transitions assign no
application commit sequence and emit no domain event.

Delivery is at least once in increasing `EventId` order within the selected
partition. A durable contiguous checkpoint plus a bounded sparse acknowledged
set permits out-of-order processing without skipping earlier work. Each item
has one attempt-specific lease token. Lease expiry makes it eligible for
redelivery; nack may release it immediately or apply a bounded delay. Ten failed
attempts move it to durable dead-letter state. Seeking requires explicit
authority and no outstanding lease.

Pull batches contain 1 through 64 events under the existing 4 MiB public
response ceiling. Long poll is at most 30 seconds. Leases range from five
seconds through fifteen minutes and default to sixty seconds. Generic consumers
default to 16 and permit at most 64 in-flight events. Contextual consumers
default to four and permit at most eight. Nack delay is at most one hour.

Consumer and lease transitions are owned by one dedicated coordinator and are
atomic in storage. Startup releases expired leases and validates checkpoint,
lease, sparse-ack, and dead-letter reciprocity. Restore history incarnation
invalidates stale external cursors and leases. A definition-hash change never
silently adopts a prior consumer checkpoint.

### Live named RiffQL

A watch executes the exact named query in one read snapshot at application head
`S`, returns that result and frontier, and then consumes durable commits strictly
after `S`. Because commit catch-up is authoritative, a commit racing snapshot
completion is not lost and watch registration is not part of a write
transaction.

Initial invalidation is conservative: point reads compare affected entity
identities where possible; otherwise any same-partition mutation of an entity
type used by the compiled access program re-executes the bounded query. Bursts
may coalesce to the latest frontier. Live queries promise convergence to current
state, not observation of every transient result.

The update vocabulary is `Snapshot`, `Patch`, `Reset`, `Checkpoint`, and
`Terminal`. A top-level many result may use insert, remove, replace, and move
patches only when its complete authorized primary key is explicitly selected.
Unkeyed results, outcome changes, excessive diffs, or buffer pressure produce a
complete reset. No internal key is exposed to make a patch possible.

One live cursor binds history incarnation, exact contract/module/query-plan
identity, canonical parameter hash, partition, and frontier. Restore, query
definition change, authorization change, cursor expiry, or incompatible role
change produces a typed reset or terminal outcome. Per database, at most 128
watches exist; one watch buffers at most 32 updates and lives at most fifteen
minutes before generated reconnect.

### Contextual agent work

A contextual subscription references one exact event stream, a bounded set of
named hydration queries, and an explicit command set. Every event-field
reference must exist with the same type across every selected event variant.
All hydration queries execute inside one shared read snapshot whose application
head is at least the triggering event sequence.

Hydrated context is freshly authorized and recomputed for every delivery. It is
not persisted and may change across redelivery. The durable event remains
stable. A work item contains the event, bounded context, currently authorized
declared commands, lease token, and a server-issued causation token.

For reaction helpers, V1 supports a command whose idempotency expression is one
direct UUID or bounded-string input. The generated client derives that value
from subscription identity, event ID, target command, and declared reaction
name. Before admitting a new command the service validates causation token,
lease, partition, history incarnation, principal, and target command. The
command provenance stores the causing event and inherited root request
correlation. An expired token may resolve an exact already-committed idempotent
outcome but cannot admit a new command.

MCP resource notifications carry only a work-available hint. Payload and
hydrated context are returned only by an authorized bounded tool call. No agent
or external callback executes inside the originating command transaction.

### Presentation and redaction

The normal event envelope exposes stable event identity and name, writer
contract version and plan, commit sequence and ordinal, logical occurrence
time, command name and request, safe actor kind, provenance locator, optional
causation, root request correlation, history incarnation, cursor, and only the
payload fields explicitly selected by the stream.

It does not expose raw principal or agent-session identities, partition or
conflict keys, unselected payload fields, credentials, or process-local trace
identifiers. Authorization is revalidated before every delivery. Revocation
closes the stream, and generated live clients clear retained protected state.
Declared output is never silently redacted.

## Compatibility

The contract grammar, contract IR/bundle, capability tags, public Protobuf,
application source/lock, storage routing and consumer records, and provenance
formats receive reviewed additive versioned successors. Existing event rows,
commit sequences, event IDs, payload hashes, application source V1 through V3,
locks V1 through V4, query modules, projections, outbox state, and kernel commit
subscription bytes retain their prior meaning.

The event-route backfill is a resumable structural migration. Reactive APIs do
not serve until its evidence is complete. Time-based event deletion and
physical retention are not implemented; authoritative history remains retained.

## Security

Event stream, live query, seek, and contextual permissions are exact
application permissions derived from symbolic definitions. MCP, CLI, gRPC, and
generated clients call the same service and policy operations. Ack and nack are
mutating MCP tools but can alter only consumer operational metadata under the
exact consumer identity. Neither cursor nor ack token is authority by itself.

Payloads and context are forbidden from telemetry. Wakeups are payload-free.
Direct browser access to riffdbd is not added; generated application server code
holds the capability and exposes an application-authenticated SSE relay.

### WP-415 public inspection boundary

The maintainer accepted this exact clarification on 2026-08-02. WP-415 exposes
one additive public `EventService` with bounded `DescribeEvent`, `ReplayEvents`,
and `TailEvents` operations through the API-neutral service, gRPC, Rust client,
and CLI. These operations expose only the ADR-0080 safe symbolic envelope and
opaque cursors; they do not expose raw commits, event payloads, partition keys,
stable numeric schema IDs, or storage identities.

WP-415 inspection is an operator surface. `DescribeEvent` requires the existing
`ReadContract` authority. `ReplayEvents` and `TailEvents` require the existing
`ReadCommit` authority because their caller selects a historical position.
They receive distinct additive service-operation and audit tags, and may not be
implemented by invoking or relabeling the existing contract or commit RPCs.

This clarification adds no capability permission or durable capability format.
WP-416 remains the sole owner of least-authority application roles bound to
named streams and reactive module identity. An application must not receive
`ReadCommit` merely to consume a named stream. WP-419 remains the owner of MCP
event tools and payload-free notification presentation.

### WP-416 reactive module and permission boundary

The maintainer accepted this exact clarification on 2026-08-02. Grammar V1
uses bounded `.riffr` sources containing named partition-local event streams,
named query watches, and contextual subscriptions. Streams select explicit
event types and fields and permit only checked bounded predicates. Watches
select `patch` or `reset`; patch mode requires the complete authorized primary
key. Contextual subscriptions bind one exact stream, bounded named hydration
queries, named command reactions, and explicit delivery limits.

Reactive source, operation, and module hashes use separate domain-separated
identities. Application Source V4, exact Application Manifest V2, and Lock V5
are additive successors. Existing Source V1 through V3, Manifest V1, and Lock
V1 through V4 bytes and meanings remain unchanged.

Capability permission tags `0x1b` through `0x1e` are respectively
`ConsumeEventStream`, `SeekEventStreamConsumer`, `WatchNamedQuery`, and
`ConsumeContextualSubscription`. Each binds exact contract lineage, reactive
module hash, and operation name. Ordinary generated roles never receive seek
authority automatically.

The fixed grammar-V1 ceilings are one partition, 32 selected event types, 256
selected fields, 16 hydration queries, 32 reactions, contextual batch and
in-flight maxima of eight, lease duration from five through 900 seconds, the
existing four-MiB public response ceiling, and the existing 500-row aggregate
query-work ceiling.

### WP-417 durable consumer and publication boundary

The maintainer accepted this exact clarification on 2026-08-02. Exact reactive
modules are published immutably through the shared control plane before a
consumer may resolve them. The catalog validates the canonical artifact against
its exact contract and query-module dependencies, storage keys it by
`ReactiveModuleHash`, and no mutable active-reactive-module pointer exists.

The additive service-operation tags `0x21` through `0x27` are respectively
`DeployReactiveModule`, `ConsumeEventStream`, `AcknowledgeEventStream`,
`NegativeAcknowledgeEventStream`, `SeekEventStreamConsumer`,
`RetireEventStreamConsumer`, and `GetEventStreamConsumerStatus`. Pull and gRPC
streaming share `ConsumeEventStream`. Consume authority covers pull, stream,
acknowledge, negative-acknowledge, and status. Exact seek authority alone covers
seek and retirement. Every operation independently reauthorizes the selected
database, lineage, module, operation, canonical parameters, partition, and
principal facts.

Durable V1 storage contains one immutable reactive-module record, one consumer
record, and one per-event delivery record. Consumer identity is the exact
`DatabaseId`, reactive-module hash, operation name, canonical query-parameter
hash, and a consumer name matching `[A-Za-z][A-Za-z0-9_-]{0,63}`. Its physical
key is a domain-separated 32-byte identity hash, while the payload repeats and
validates the complete identity. Consumer state carries a nonzero compare-and-
transition revision, one contiguous checkpoint, and at most 64 canonically
ordered sparse terminal resolutions. Per-event delivery state is exactly
leased, retry-ready or retry-delayed, or dead-lettered; it never duplicates an
event payload.

One lease has a random 32-byte opaque attempt token generated at the existing
server entropy boundary and stored durably. The token binds the consumer,
event, attempt, and history incarnation but is never authority by possession.
New first attempts are leased in increasing `EventId` order. A retry may revisit
an older event. Ack and dead-letter are the only terminal resolutions; ten
expired or negatively acknowledged attempts dead-letter the event, and that
terminal disposition participates in contiguous checkpoint advancement.

Seek requires no live lease and atomically sets a checkpoint at before-first or
one selected event while removing later sparse, retry, and dead-letter state.
Retirement has the same no-live-lease and seek-authority preconditions and
atomically removes the consumer plus all delivery metadata. Ordinary restart
retains unexpired leases and converts expired attempts to retry or dead-letter
state. Restore-incarnation change invalidates every live lease and old external
token while retaining checkpoint, retry, sparse-resolution, and dead-letter
truth restored from the backup.

Consumer transitions assign no application sequence and mutate no application
entity. `riffdb-service` owns the one API-neutral consumer coordinator and its
clock/token source boundaries; it receives only a narrow consumer-owned port.
`riffdb-storage-api` owns the atomic semantic transitions and durable records,
and memory/redb implement them. Catalog owns exact module publication and
resolution. Commit owns the control-plane publication transition. Server and
transport code only compose or adapt those owners.

## Rejected alternatives

- Raw CDC or public commit subscriptions as the application event interface.
- A second event store or broker-owned event identity.
- Independent event schema versions in addition to contract versions.
- Persisting hydrated agent context or requiring historical entity snapshots.
- Exactly-once delivery or exactly-once external effects.
- Global ordering, cross-partition streams, or time-based event deletion.
- Direct browser database credentials.
- Running LLM inference or arbitrary callbacks inside RiffDB.
- Building connectors, webhooks, or declarative reactions before the RT-1
  through RT-5 acceptance gate.

## Acceptance

- Compiler diagnostics cover unpartitioned, mismatched, cross-partition,
  unbounded, unauthorized, and unkeyed definitions with source spans.
- Storage and recovery tests cover route backfill, leases, sparse ack,
  dead-letter, restore incarnation, corruption, and every crash boundary.
- A barrier-controlled commit between query snapshot and catch-up is delivered.
- Applying every patch/reset yields the same result as a fresh exact query.
- Revocation, definition changes, restore, and buffer pressure close or reset
  without leaking retained values.
- Killing a contextual consumer after command commit and before ack redelivers
  the event, resolves the original command outcome, and duplicates no state.
- TicketDesk works through generated Rust, TypeScript, Python, CLI, and MCP
  without numeric IDs, raw commit rows, encoded keys, or handwritten transport.
