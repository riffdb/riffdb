# WP-473: Sealed command-audit link evidence

WP-473 removes redundant commit, event, and provenance row decoding from the
successful command transaction's terminal-audit validation. It changes only a
private in-memory proof handoff inside the redb adapter. Durable records, keys,
bytes, transaction membership, ordering, acknowledgement, uncertainty, and
recovery behavior are unchanged.

## Sealed same-transaction path

Every command graph still passes complete reciprocal semantic validation,
canonical encoding, reservation checks, and physical insertion. Only after all
of those inserts succeed does the redb command typestate consume the checked
graph into move-only staged evidence. Consuming that evidence for publication
derives a second move-only value containing the exact outcome-owned commit
sequence and provenance identity.

The terminal-audit group requires one such value for every `Command` link in
the same command order. Missing, extra, mismatched, or reordered evidence fails
the transaction before any audit row is appended. The evidence has private
fields, is neither cloneable nor constructible by the redb adapter, and its
debug representation is fully redacted.

This proof is accepted only by the two successful command commit paths:
direct and unpublished-before-fence. Architecture coverage fixes those call
sites and proves that the physical graph insertion precedes creation of staged
evidence. Lifecycle reconstruction, administration-tail and allocator checks,
request-index writes, canonical audit encoding, and atomic commit behavior are
unchanged.

## Full authoritative fallback

An independently submitted service-audit link does not possess staged command
evidence. It therefore retains the complete authoritative path: read and
canonically decode the commit, load and validate every referenced event row,
decode provenance, and compare both reciprocal identities. Startup, recovery,
migration, and retained-history validation likewise continue to decode durable
bytes. A process or transaction loss cannot recreate the sealed proof and
cannot enter the optimized path.

The recovery matrix includes an exact two-command counterexample that swaps
the audit transitions relative to the staged command graphs. The link mismatch
returns `InvariantViolation`, appends no audit rows, and leaves both command
graphs absent.

## Evidence

The full generated-Rust/public-gRPC TicketDesk seed contains 19,220 ordinary
commands at generated concurrency 384. The retained WP-472 reference was
2.332 seconds. Two adjacent WP-473 runs completed in 2.134 and 2.124 seconds,
an approximately nine-percent reduction. Their writer traces were:

| Metric | WP-472 | WP-473 run 1 | WP-473 run 2 |
| --- | ---: | ---: | ---: |
| Validation, encoding, and staging | 0.794 s | 0.779 s | 0.824 s |
| Commit/fence | 1.162 s | 1.002 s | 1.057 s |
| Full seed | 2.332 s | 2.134 s | 2.124 s |

The commit/fence reduction reflects less transaction-current B-tree read and
decode work before redb publishes the same writes; the staging-stage samples
remain within same-host variance. A pre-change 512-concurrency probe completed
in 2.335 seconds with 149 physical groups, confirming that increasing bounded
client ingress did not remove the cost.

The final representative unary medians were 1.681 ms for `create_comment`,
1.799 ms for `close_ticket_with_comment`, 1.621 ms for
`swap_member_roles`, and 1.791 ms for `open_ticket_with_labels`. No unary
regression was retained.

These measurements are short same-host engineering evidence, not a published
cross-database comparison.
