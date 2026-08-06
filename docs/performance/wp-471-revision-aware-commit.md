# WP-471: Revision-aware commit materialization

WP-471 removes one deep event-collection clone and speculative compatibility
dispatch from the current commit-row materialization path. It changes only the
in-memory durable-codec handoff used after a commit row has already been read.
Durable schemas, bytes, keys, event-table authority, transaction ordering,
audit linking, acknowledgement, and recovery behavior are unchanged.

## Closed revision witness

The event-reference decoder now returns one closed revision witness with its
ordered checked references:

- `V3` for the current payload-free commit containing entity and event
  references;
- `V2` for the historical commit containing entity post-images and event
  references; or
- `LegacyV1` for the historical commit containing embedded post-images and
  events.

The witness is produced only by decoding the same canonical envelope bytes that
will be materialized. It is not supplied by an application or inferred from
database metadata. Supplying a witness for different bytes fails with the
existing unexpected-record-type error.

Redb uses the witnessed ordered references to load each authoritative event
row once and validate its exact event ID and hash. It then moves that event
collection into the one matching semantic decoder. Current V3 and historical
V2 still reconstruct and validate the complete commit. Legacy V1 still decodes
its complete embedded event collection and compares it exactly with the event
table before returning the record.

Unknown types, malformed envelopes, checksum or schema mismatches,
noncanonical bytes, malformed payloads, absent events, reference/hash drift,
and invalid semantic records therefore retain the same fail-closed behavior.
Compatibility tests construct all three readable revisions, assert the exact
witness and encoded charge, compare direct materialization with the prior
compatibility decoder, and reject a cross-row witness substitution.

## Evidence

The full generated-Rust/public-gRPC TicketDesk seed contains 19,220 ordinary
commands at generated concurrency 384. WP-470's two adjacent final runs were
2.411 and 2.424 seconds. The three-repetition WP-471 report produced a
2.371-second median. Its representative writer trace reported:

| Writer stage | WP-470 | WP-471 |
| --- | ---: | ---: |
| Validation, encoding, and staging | 0.828 s | 0.810 s |
| Commit/fence | 1.241 s | 1.215 s |
| Full seed | 2.424 s | 2.371 s |

Representative post-seed unary medians were 1.826 ms for `create_comment`,
2.644 ms for `close_ticket_with_comment`, 1.686 ms for `swap_member_roles`, and
1.808 ms for `open_ticket_with_labels`. The multi-entity close result is within
the existing same-host variance; the other samples show no material regression.

WP-471 is retained as a small allocation and dispatch improvement. Current
commit materialization still parses the commit once to discover references and
again to construct the complete semantic record. Eliminating that remaining
duplicate parse would require a separately reviewed prepared-decoder boundary;
WP-471 does not weaken full materialization to obtain it.

These measurements are short same-host engineering evidence, not a published
cross-database comparison.
