# ADR-0035: Atomic Authoritative Scan Fences

- **Status:** Accepted
- **Direction approved:** 2026-07-21
- **Exact text accepted:** 2026-07-21
- **Accepted:** 2026-07-21
- **Requires:** ADR-0003, ADR-0004, ADR-0006, ADR-0007, ADR-0028
- **Amends:** ADR-0004 authoritative scan API and ADR-0028 `IndexScanFence`
- **Decision deadline:** Before WP-130 implements production authoritative reads

The human maintainer accepted this exact decision and its authoritative-file
amendments on 2026-07-21.

## Context

The API-neutral service already requires two facts that the lower storage scan
API does not return:

- an index page and the exact range epoch observed atomically with its rows;
- an initial commit page and the authoritative upper frontier frozen for every
  continuation.

Reading either fact in a separate transaction races with a commit. Scanning a
changing commit log to its eventual end is unbounded. The server cannot infer a
fence without weakening cursor consistency.

A second conflict exists at the empty index state. Storage normatively uses
`IndexEpochPosition::BeforeFirst` until the first mutation assigns epoch one,
but the service and completed phase-zero public schema require a nonzero
`IndexEpoch`. Mapping `BeforeFirst` to epoch one is unsound: the first mutation
would leave the visible fence unchanged and could fail to invalidate a cursor.

## Decision

### Shared epoch position

`riffdb-types` owns the closed semantic value:

```rust
pub enum IndexEpochPosition {
    BeforeFirst,
    Value(IndexEpoch),
}
```

`riffdb-storage-api` re-exports that type for source compatibility and no crate
defines a second index-position type. The existing durable
`riffdb.storage.v1.IndexEpochPositionV1` encoding and tags do not change.

### Storage index pages

Every `AuthoritativeIndexScanPage` variant carries an
`IndexEpochPosition`. A checked `epoch()` accessor returns it. Redb opens the
secondary-index and index-epoch tables in one read transaction and reads both
the target epoch and bounded rows from that transaction. Memory returns both
under one immutable locked view.

### Storage commit pages

`CommitScanRequest` is closed:

```rust
pub enum CommitScanRequest {
    Initial { limit: StorageScanLimit },
    Continue {
        after: CommitSequence,
        inclusive_upper: CommitSequence,
        limit: StorageScanLimit,
    },
}
```

The continuation constructor requires `after <= inclusive_upper`.
`CommitScanPageV1` carries `inclusive_upper: FrontierPosition`. For an initial
scan, redb obtains `COMMITS.last()` and scans rows in that same read transaction.
For a continuation it scans only `after < sequence <= inclusive_upper`; commits
appended later are ignored. Memory provides equivalent snapshot semantics.

Checked page construction requires bounded encoded content, contiguous
sequences, no row above the fence, a continuation exactly at the last returned
row only when the fence has not been reached, and exact end only when the
returned or prior position reaches the fence. An empty initial log returns
`FrontierPosition::BeforeFirst`.

### Service and public index fence

`riffdb-service::AuthoritativeIndexPage` and `IndexScanFence` carry the shared
`IndexEpochPosition`. The server adapter mechanically preserves the lower
position; it never substitutes a value.

Before the first supported gRPC server, the phase-zero public message is
corrected to a closed position and the old scalar tag is reserved:

```protobuf
message IndexScanFence {
  reserved 1;
  reserved "index_epoch";
  oneof position {
    Unit before_first = 2;
    uint64 applied_epoch = 3;
  }
}
```

Exactly one member is required and `applied_epoch` is nonzero. This is an
explicit pre-release correction to the WP-127 fixture boundary. Descriptor,
schema-hash, golden, strict-decoder, generated Tonic, and SDK fixtures are
regenerated together. No released operation or durable byte format is migrated.

## Options Considered

1. **Atomic lower results and a closed public epoch position:** Proposed. It
   represents the real empty state and gives cursors a truthful fence.
2. **Read epoch/head separately:** Rejected. A commit may occur between reads.
3. **Map `BeforeFirst` to epoch one:** Rejected. The first mutation would not
   advance the apparent fence.
4. **Use scalar zero as a public sentinel:** Rejected. It permanently conflates
   omitted scalar encoding with a semantic state when the existing protocol
   already uses closed oneofs for before-first frontiers.
5. **Hold a read transaction across service pagination:** Rejected. It leaks a
   storage transaction across policy, network, and caller think time.
6. **Scan until the moving log end:** Rejected. It is unbounded and cannot give
   a stable continuation contract.

## Consequences

- WP-060/WP-070 receive a focused semantic read-interface correction covering
  storage API, memory, redb, and recovery fixtures.
- WP-120 can implement its already declared page fences without inference.
- WP-127 regenerates one not-yet-served public message and its compatibility
  fixtures under explicit human review.
- WP-130's read adapter is mechanical and holds no storage transaction across a
  service or transport boundary.
- Application commits, commit ordering, durable records, and coordinator
  ownership do not change.

## Testing

Storage tests cover empty `BeforeFirst`, epoch-plus-row consistency, frozen
initial commit head, later append exclusion, bounded continuation, sequence
gaps, rows above the fence, and exact-end truthfulness for memory and redb.
Service tests preserve the lower position in cursors. Proto and gRPC tests cover
both position branches, unset/duplicate/zero rejection, generated-artifact
reproducibility, and first-mutation cursor invalidation.

## Requirements and Work Packages

- **Requirements:** `STO-001`, `STO-012`, `API-001`, `REC-001`, `REC-002`
- **Corrects:** `WP-060`, `WP-070`, `WP-120`, and `WP-127`
- **Blocks:** `WP-130`
- **Final evidence:** `WP-130` and `WP-200`
