# WP-374 — Authoritative event references

WP-374 makes the `events` table the sole durable owner of each complete event
payload. Durable commit and outbox rows now store an `EventReferenceV2`
containing the exact `EventId` and `EventHash`; the semantic storage ports still
materialize complete checked records so callers do not gain a payload-free
escape hatch.

The commit coordinator stages the event row and both reciprocal references in
the same authoritative transaction. Reads, outbox dispatch, projection replay,
backup inspection, administration lookup, and startup recovery load the event
from the same redb snapshot and verify both reference fields before releasing
it. Missing rows, reordered references, ID substitution, or hash substitution
are authoritative corruption rather than recoverable absence.

## Compatibility

The predecessor WP-373 registry digest is frozen as
`79c2c86527e0b83f67ede5a74aaf750ac19438d9c90e45e0cb7b4f18e2ddc99e`.
Only that exact registry may enter the semantic migration. Migration is bounded
by both row and byte limits, accepts mixed migrated/unmigrated rows on restart,
and publishes the new registry digest only after every commit and outbox row
has been decoded against its authoritative event and rewritten. An unknown
registry remains an incompatible format.

The recovery test interrupts migration after an uncertain durable batch,
reopens the database, completes the remaining work, and proves both new rows
against the unchanged event row before observing the new registry marker.

## Byte evidence

The checked compatibility vector with one small event has these complete
compact-envelope sizes:

| Durable row | WP-373 bytes | WP-374 bytes |
| --- | ---: | ---: |
| Authoritative event | 90 | 90 |
| Commit | 517 | 481 |
| Outbox intent | 92 | 56 |
| Total | 699 | 627 |

That small vector saves 72 bytes (10.3%). The savings grow with event payload
size because WP-373 stored the complete event three times while WP-374 stores
the payload once and two fixed-size references. Exact V1 and V2 compatibility
bytes are frozen under `fixtures/proto/`.

This change does not remove the complete event from transient command graphs or
owned semantic return values. Those are bounded in-memory values used to prove
atomic reciprocity; they are not additional durable payload owners.
