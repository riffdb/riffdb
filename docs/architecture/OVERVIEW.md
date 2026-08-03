# System Overview

RiffDB is a standalone Rust process with strict ownership boundaries around
public input, deterministic evaluation, and durable authority.

![RiffDB proof-of-concept architecture](../assets/poc-architecture.svg)

## Public edge

The gRPC server, native MCP adapter, CLI, and generated clients adapt public
requests into API-neutral service operations. They do not own business
semantics or access storage directly. The shared service performs schema-bound
materialization, authentication, authorization, catalog selection, limits, and
redaction.

## Semantic core

The contract compiler owns the transition from source syntax to typed IR and
checked executable plans. The runtime evaluates only compiler-produced plans
against a read snapshot and produces an intent. It has no access to network,
filesystem, operating-system time, process-global mutation, or untracked
randomness.

The conflict manager owns in-memory logical capabilities. It improves admission
and ensures stable acquisition order; authoritative correctness still depends
on transaction-current validation.

## Durable core

The commit coordinator is the only owner allowed to assign application commit
sequences or apply authoritative mutations. Storage exposes semantic snapshot,
scan, and atomic-commit operations rather than arbitrary transaction callbacks.
The redb implementation is replaceable behind that boundary, but durable
encodings and compatibility fixtures are explicit product contracts.

## Derived workers

Outbox delivery and projections consume authoritative commits. Outbox intent is
atomic with a command, while external delivery is not. Projection state is
versioned by generation and frontier and can be rebuilt from authoritative
history.

## Reactive application path

The accepted P8 architecture reuses authoritative domain events; it does not
create a second event store. The current compiler-proved event partition and
derived route-index foundations let the catalog materialize bounded symbolic
replay from immutable events, commits, writer plans, and provenance. The route
index owns ordering evidence, not payload bytes.

Durable consumer state is operational metadata owned by a dedicated
service coordinator and semantic storage operations. Lease, acknowledgement,
retry, dead-letter, and seek transitions will assign no application commit
sequence and cannot mutate entity state. Possessing a cursor or lease token
will grant no authority; the service reauthorizes the exact database, reactive
definition, partition, and principal for every operation.

A live named query executes once in a consistent snapshot at frontier
`S`, then catches up from authoritative commits after `S`. Compiler-derived
invalidation may conservatively rerun the bounded query, while public clients
receive only the closed snapshot, patch, reset, checkpoint, and terminal
variants. Contextual agent work will compose one durable event with freshly
authorized named-query hydration in one snapshot. Only a later reaction command
writes authoritative state, with causing-event provenance committed atomically
through the normal command path.

The current live-query adapter is gRPC over the shared API-neutral service.
WP-419 adds CLI, generated SDK, MCP, and application-owned browser relay
adapters over those same semantics. MCP notifications remain payload-free
wakeups, and browser clients never receive a RiffDB capability or direct
database connection.

## Crate ownership

Workspace crates are internal architecture boundaries unless specifically
published. Application authors use generated modules and
`riffdb-client-rust`; Python and TypeScript packages expose equivalent stable
application surfaces. Internal crates do not become public merely because
Rustdoc can build them.
