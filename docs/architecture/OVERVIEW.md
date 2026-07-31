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

## Crate ownership

Workspace crates are internal architecture boundaries unless specifically
published. Application authors use generated modules and
`riffdb-client-rust`; Python and TypeScript packages expose equivalent stable
application surfaces. Internal crates do not become public merely because
Rustdoc can build them.
