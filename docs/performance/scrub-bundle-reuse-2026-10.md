# Scrub Bundle and Row Reuse (WP-833)

ADR-0286 lets a complete structural walk reuse the last contract bundle it
validated when a later `COMMITS` row's stored bundle record is identical, and
lets the walk's command history reuse the commit phase's decode of a
byte-identical row. The commit phase previously decoded and validated the
command's whole contract bundle on every row, and the history decoded each row
a second time. The background scrub and complete startup validation both use
the reuse. Every finding, refusal and member is unchanged.

## Measurement

Second development machine (8 vCPU, AMD EPYC 7B12), TicketDesk `full` dataset,
write-only load, alternating base and candidate daemons, three rounds of two
arms each. The value is the measured process generation's
`background_scrub_total_us`, with the harness waiting for the scrub before load.

| build | scrub seconds (six runs) | median |
|---|---|---:|
| base (`main` before WP-833) | 9.0, 8.1, 7.9, 8.0, 7.9, 8.2 | 8.1 |
| WP-833 | 5.7, 5.9, 5.8, 5.7, 5.7, 6.0 | 5.8 |

The scrub takes 28% less time. A separate series with only the bundle reuse
measured 8.3 s on the base and 7.3 s with it. Before the change, a
frame-pointer profile put 17% of the scrub thread's samples in per-row bundle
validation and 19% in the history's second row decode. Steady-state
throughput after the scrub is unaffected: no ordinary read or write path uses
the reuse.

## Not changed

The entity-chain and historical passes still decode each segment once each,
and the duplicate work ADR-0283 left (a second SHA-256 over each resolved
frame, and receipts encoded only to be decoded) remains.
