# ADR-0004: Semantic Storage API and Redb Baseline

- **Status:** Proposed
- **Direction approved:** 2026-07-12
- **Exact text accepted:** No
- **Decision deadline:** Before WP-060 public traits merge

The human architecture review approved this direction. This record remains
Proposed until its exact text is reviewed and accepted.

## Context

The runtime must be deterministic and storage-neutral, while commit-time
revalidation and atomic persistence require a short engine transaction. A broad
transaction callback or a live redb handle in the runtime would obscure
ownership, hold writer transactions across evaluation, and couple semantics to
one engine.

## Proposed Decision

`riffdb-storage-api` owns synchronous, bounded, engine-neutral `ReadSnapshot` and
`StorageEngine` semantics plus the value-only `CommitIntent` contract. Runtime
constructs `CommitIntent` from a bounded materialized snapshot and explicit
transaction context. It performs no storage I/O and holds no storage transaction.

After evaluation, the commit coordinator opens a narrowly typed, short-lived
write transaction through the storage API. Within it the coordinator directs
idempotency recheck, dependency validation, exact predicate validation, sequence
allocation, authoritative mutations, persisted outcome, events, provenance,
outbox intent, and commit record, then atomically commits. The interface exposes
no arbitrary callback, raw transaction, or engine type.

Specialized catalog, capability, outbox, and projection operations are semantic
subinterfaces with bounded records and transitions. Authoritative control-plane
changes use typed coordinator operations. Projection state remains derived.
Redb is the POC production implementation; the memory implementation and the
non-production Fjall experiment run the same conformance suite.

## Options Considered

1. **Materialized snapshot plus coordinator-directed narrow transaction:**
   Approved boundary.
2. **Live storage snapshot through runtime:** Fewer copies but risks transaction
   lifetime leakage and engine coupling.
3. **Engine-owned semantic `commit(intent)`:** Makes the engine the effective
   coordinator.
4. **General transaction callback:** Expands scope into arbitrary transactions and
   weakens compile-time ownership.

## Consequences

- Snapshot materialization, collections, scans, and intent size are bounded.
- Revalidation reads happen again inside a short write transaction.
- Storage backends implement semantics, not merely key-value operations.
- New durable transitions require storage-interface and coordinator review.

## Compatibility

Public storage traits, key encodings, table records, and transactional transition
semantics must be frozen with conformance and durable fixtures. Redb types never
cross the adapter crate.

## Security

The API applies redaction before safe errors or diagnostics and does not expose
raw capability tokens. Transaction closure cannot be supplied by untrusted or
transport code.

## Testing

Run one parameterized semantic suite over memory, redb, and the isolated Fjall
experiment where applicable. Include bounded snapshot, absence/version/range,
atomic state/model equality, transaction-duration instrumentation, failpoints,
process kill/reopen, and architecture dependency tests.

## Requirements and Work Packages

- **Requirements:** `STO-001`, `STO-002`, `STO-010` through `STO-012`,
  `STO-020` through `STO-022`, `ENT-001` through `ENT-004`, `EFF-001`
- **Defines or blocks:** `WP-060`, `WP-070`, `WP-075`, `WP-080`, `WP-100`
- **Final evidence:** `WP-190`, `WP-200`

## Decision Deadline

Exact acceptance is required before WP-060 merges any public storage trait or
commit-intent type. Prototypes may live only in ADR discussion or tests.
