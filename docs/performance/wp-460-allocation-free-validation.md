# WP-460: Allocation-free reciprocal record validation

Complete command-record construction validates that entity mutations, durable
events, outbox intents, provenance, and the commit record name exactly the same
bounded identities in canonical order. Those checks remain mandatory before
storage sees the graph.

WP-460 removes allocation that contributes no additional evidence. Reciprocal
sequences are compared directly with bounded iterators rather than copied into
temporary vectors. After physical staging, the backend evidence takes ownership
of the exact provenance event-identity vector that passed those checks rather
than allocating and copying a replacement.

This is an ownership and comparison optimization only. It does not skip a
comparison, trust external bytes, change a durable codec, alter transaction
ordering, or move validation after commit. The retain-or-revert gate is the
full public TicketDesk seed at generated concurrency 128, compared with the
retained WP-459 build.

## Evidence

The retained comparison uses adjacent full public runs on the same loaded host.
Absolute flush time was elevated in both runs, so the comparison is a
retain-or-revert guard rather than a new release baseline.

| Build | Full seed | validation / encoding / staging | Physical commits | Unary `create_comment` p50 |
|---|---:|---:|---:|---:|
| WP-459 | 5.977 s | 1.272 s | 342 | 5.241 ms |
| WP-460 | 5.905 s | 1.299 s | 344 | 4.330 ms |

Total seed time improved 1.2%; the targeted aggregate stage moved by 2.1%,
inside the run-to-run noise visible in physical group count and flush time, and
unary latency did not regress. The change is retained because it proves and
removes up to four allocations per event-emitting command without altering semantic
work, while the full public path remains neutral. Fsync count and time still
dominate the result.
