# WP-472: Single-pass commit materialization

WP-472 removes the remaining duplicate durable-envelope and Protobuf decode
from commit-row materialization. It changes only the in-memory codec handoff
between the canonical commit decode and redb's authoritative event-table join.
Durable schemas, bytes, keys, event-table authority, transaction ordering,
audit linking, acknowledgement, and recovery behavior are unchanged.

## Prepared decode boundary

The canonical readable decode now returns one move-only prepared value for the
exact revision found in the row:

- current V3 retains its decoded entity and event-reference payload;
- historical V2 retains its decoded entity-post-image and event-reference
  payload; and
- legacy V1 retains its complete semantic record with embedded events.

The payload is private to the first-party codec. Its debug representation
contains only the closed revision and bounded event-reference count. It cannot
expose event identities, hashes, actor values, or business data, and callers
cannot use it to bypass the authoritative event-table join.

Redb borrows the prepared references to load each authoritative event row once
in order and performs the same exact event-ID and content-hash comparison. It
then consumes the prepared value and loaded event collection once. V3 and V2
perform all existing field conversion and `StoredCommitRecordV1` semantic
construction checks. Legacy V1 performs the same complete embedded-event
equality comparison.

The direct V3, V2, and legacy decoders remain available for compatibility
coverage. Tests prove that prepared materialization returns the same semantic
record and encoded charge for every readable revision, that debug output is
redacted, and that absent authoritative events fail closed. Existing checksum,
schema, canonical-encoding, malformed-payload, reference/hash, semantic, and
recovery suites remain authoritative.

## Evidence

The full generated-Rust/public-gRPC TicketDesk seed contains 19,220 ordinary
commands at generated concurrency 384. The adjacent WP-471 retained run was
2.371 seconds. The final WP-472 run was 2.332 seconds. Its representative
writer trace reported:

| Writer stage | WP-471 | WP-472 |
| --- | ---: | ---: |
| Validation, encoding, and staging | 0.810 s | 0.794 s |
| Commit/fence | 1.215 s | 1.162 s |
| Full seed | 2.371 s | 2.332 s |

Representative post-seed unary medians were 1.938 ms for `create_comment`,
1.777 ms for `close_ticket_with_comment`, 1.777 ms for
`swap_member_roles`, and 1.883 ms for `open_ticket_with_labels`. These remain
within the adjacent same-host range and show no material regression.

The first WP-472 run was 2.411 seconds and demonstrated normal same-host
variance; the final run after restoring the prior validation order is the
retained evidence. This is a small CPU and allocation improvement, not a new
durability mechanism. Remaining seed time is dominated by commit/fence work
and command-graph validation, encoding, and staging rather than duplicate
commit-row parsing.

These measurements are short same-host engineering evidence, not a published
cross-database comparison.
