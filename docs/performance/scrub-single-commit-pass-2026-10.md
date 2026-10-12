# Scrub Single Commit Pass (WP-834)

ADR-0287 lets a complete structural walk build its command history, entity
chains and the historical plan's `COMMITS` plan references in one forward
pass, decoding each command segment once for all three. The walk previously
read `COMMITS` four times; it now reads it twice (this pass and the commit
phase). The background scrub and complete startup validation both use it.
Every finding, refusal and member is unchanged.

## Measurement

Second development machine (8 vCPU, AMD EPYC 7B12), TicketDesk `full` dataset,
write-only load, alternating base and candidate daemons, three rounds of two
arms each. The value is the measured process generation's
`background_scrub_total_us`, with the harness waiting for the scrub before load.

| build | scrub seconds (six runs) | median |
|---|---|---:|
| base (`main` before WP-834) | 6.8, 6.5, 6.7, 6.6, 6.2, 5.7 | 6.6 |
| WP-834 | 4.1, 3.8, 7.0, 4.1, 3.6, 3.5 | 3.9 |

The scrub takes 40% less time. The machine was noisier than in earlier
series: the same base measured 5.8 s in the WP-833 series. A frame-pointer
profile taken from the scrub's first instruction put 18% of the scrub
thread's samples in the history pass, 21% in the entity-chain pass and 24% in
the plan pass before the change; the commit phase (30%) is unchanged.
Steady-state throughput after the scrub is unaffected.

## Not changed

The commit phase still decodes each segment itself, and the duplicate work
ADR-0283 left (a second SHA-256 over each resolved frame, and receipts encoded
only to be decoded) remains.
