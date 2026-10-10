# Scrub Segment Reuse (WP-831)

ADR-0283 lets a complete structural walk reuse the last command segment it
decoded when a later lookup reads byte-identical `COMMITS` row bytes. The
background scrub and complete startup validation use it for the service-audit
link checks, which previously decoded a command's whole segment, resolving its
history stub from the journal, several times per command. Every finding,
refusal and member is unchanged.

## Measurement

Second development machine (8 vCPU, AMD EPYC 7B12), TicketDesk `full` dataset,
write-only load, alternating base and candidate daemons, three rounds of two
arms each. The value is the measured process generation's
`background_scrub_total_us`, with the harness waiting for the scrub before load.

| build | scrub seconds (six runs) | median |
|---|---|---:|
| base (`main` before WP-831) | 20.1, 23.1, 16.8, 21.1, 18.9, 19.5 | 19.8 |
| WP-831 | 10.1, 9.6, 9.4, 8.9, 9.2, 9.3 | 9.3 |

The scrub takes 53% less time. Before the change, a frame-pointer profile put 70% of the scrub's
samples in the repeated segment decodes under `service_link_is_valid`. Steady-state
throughput after the scrub is unaffected: no ordinary read or write path uses
the reuse.

The scrub's `background_scrub_rows` and `background_scrub_rows_per_second`
evidence fields previously summed the session's running total after every page.
They now report the running total; values banked earlier overstate them.

## Not changed

Each resolved history stub is still hashed twice (physical and logical frame
checksums), and each rebuilt receipt is still encoded only to be decoded. Both
are smaller than the removed decodes and are left for a separate decision.
