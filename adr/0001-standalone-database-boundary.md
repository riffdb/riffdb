# ADR-0001: Standalone Database Boundary

- **Status:** Proposed
- **Direction approved:** 2026-07-12
- **Exact text accepted:** No
- **Decision deadline:** Before claiming the P0 gate

The human architecture review approved this direction. This record remains
Proposed until its exact text is reviewed and accepted.

## Context

RiffDB must prove that contract-first operational semantics belong to the
database, including deterministic commands, durable outcomes, provenance,
idempotency, and derived frontiers. Implementing the POC as an extension of an
existing database or as an external semantic control plane would leave ownership
of key atomicity and recovery guarantees unclear. Replication and distributed
transactions are explicitly outside the POC.

## Proposed Decision

RiffDB is a standalone single-node Rust database server. It owns the authoritative
entity state, commit log, outcomes, events, provenance, catalog, and security
state required by the POC. Redb is the initial storage engine behind a semantic
storage interface; it is not RiffDB's public API.

PostgreSQL is permitted only in the isolated comparison application. It is not a
runtime dependency, alternate authoritative store, extension host, or control
plane. Replication-shaped records may preserve a future path, but the POC adds no
Raft, consensus, partitioning, or distributed transaction abstraction.

## Options Considered

1. **Standalone server:** Directly proves RiffDB's ownership of semantics and is
   the approved POC boundary.
2. **PostgreSQL extension:** Reuses infrastructure but entangles the proof with
   another database's transaction and extension model.
3. **External semantic control plane:** Leaves authoritative writes in another
   system and cannot establish the intended atomic boundary.

## Consequences

- `riffdbd` owns lifecycle and durable recovery for one local node.
- Storage adapters remain private implementation choices behind conformance tests.
- Operational features supplied by mature databases must be built or deferred
  explicitly.
- Replication and partitioning remain post-POC work.

## Compatibility

The standalone process, public gRPC API, and durable RiffDB directory become
product boundaries. No PostgreSQL compatibility promise is created by the
comparison example.

## Security

The server owns authentication handoff, authorization, capability persistence,
redaction, and audit. An external database role or connection is never an
authorization bypass.

## Testing

Architecture checks reject PostgreSQL and storage-engine dependencies from
first-party semantic and transport crates. WP-200 runs the POC from a fresh local
database and reports comparison workloads separately.

## Requirements and Work Packages

- **Requirements:** `SYS-001`, `SYS-002`, `SYS-004`, `REP-001`
- **Defines or blocks:** documentation in `WP-000`; P0 gate interpretation
- **Final evidence:** `WP-190`, `WP-200`

## Decision Deadline

Exact acceptance is required before P0 is claimed. WP-000 may create only the
non-semantic standalone workspace skeleton while this record is Proposed.
