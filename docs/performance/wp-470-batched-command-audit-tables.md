# WP-470: Batched command-audit table access

WP-470 removes repeated redb table-open and handle-construction work from the
bounded admission and terminal-audit groups already selected by the commit
coordinator. It changes transaction-local physical access only. It does not
change command grouping, logical transactions, durable encodings, validation,
authorization, acknowledgement, or recovery semantics.

## Admission path

One bounded admission group now opens the pending-idempotency, terminal-
idempotency, contract-retirement, and contract-bundle tables once. Each command
still performs its own exact candidate lookup, multiple-match rejection,
terminal replay comparison, retirement check, bundle identity validation,
canonical pending-record encoding, and insert assertion.

Audited admission likewise opens the service-audit and request-index tables
once. It gathers the bounded request-index results in one ordered read, then
validates every requested lifecycle independently before staging any admission.
A missing lifecycle, duplicate request, invalid principal, non-`Started` phase,
or non-empty link still rejects the complete storage operation before commit.

## Terminal-audit path

One terminal command group now opens the commit, event, and provenance tables
once and passes those transaction-local handles through each link validation.
For every command, storage still:

- resolves the exact commit sequence;
- decodes the complete commit and its event records;
- checks the linked provenance identity on the commit;
- resolves and decodes the provenance record; and
- checks the reciprocal commit sequence and provenance identity.

The group also opens `AUDIT_BY_REQUEST` once. Every audit row still receives an
independently encoded, self-verifying request-index row, and any replacement of
an existing key remains corruption. Lifecycle validation, administration-
sequence allocation, canonical audit encoding, and the atomic
command-plus-audit transaction are unchanged. Standalone service-audit writes
retain their single-operation helper.

## Evidence

The full generated-Rust/public-gRPC TicketDesk seed contains 19,220 ordinary
commands at generated concurrency 384. The retained WP-469 baseline reported a
2.482-second median and roughly 0.20 seconds in admission. Intermediate WP-470
runs completed in 2.396 and 2.386 seconds. Two adjacent runs of the complete
admission-plus-terminal implementation completed in 2.411 and 2.424 seconds.
The latter trace reported:

| Writer stage | Elapsed |
| --- | ---: |
| Admission | 0.168 s |
| Compatibility | 0.017 s |
| Evaluation | 0.116 s |
| Validation, encoding, and staging | 0.828 s |
| Commit/fence | 1.241 s |

Representative post-seed unary medians in that run were 2.000 ms for
`create_comment`, 1.828 ms for `close_ticket_with_comment`, 2.652 ms for
`swap_member_roles`, and 2.420 ms for `open_ticket_with_labels`. These remain in
the normal same-host run-to-run range. The work is retained as a modest physical
CPU reduction: it keeps the public seed near 2.4 seconds and removes redundant
table setup without weakening the checks whose cost is still visible.

The remaining critical path is dominated by redb commit/fence work and by
validation, encoding, and staging. WP-470 deliberately does not cache decoded
authoritative records, remove revalidation, combine identities, or weaken
canonical-byte checks to reduce those costs. Any such semantic change requires
separate specification and architecture review.

These measurements are short same-host engineering evidence, not a published
cross-database comparison.
