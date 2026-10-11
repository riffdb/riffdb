# Outbox Claim Groups (WP-832)

ADR-0284 lets the outbox dispatcher claim up to sixteen consecutive pending
events of a scan page in one primary transaction, instead of one transaction
per claim. Every outbox write takes the sole writer gate, applies the published
command journal suffix and commits Immediate, so before this change each
delivered event cost one gate turn that command epochs queued behind.

## Measurement

Second development machine (8 vCPU, AMD EPYC 7B12), observed TicketDesk
lifecycle preparation with online retention: 64 organizations, 65,536 seeded
tickets, 300 seconds at 100 commands/s and 400 reads/s with 28 load slots,
the durable local archive and a live consumer. Two runs of each build,
alternating, from the same source except WP-832. An uncommitted scratch build
attributed every command-epoch wait for the writer gate to the class of writer
holding it; the figures below average the ten 30-second intervals of the load
window.

| build | p95 | p99 | over 250 ms | outbox writes / 30 s | outbox gate hold / 30 s | command wait behind outbox / 30 s |
|---|---:|---:|---:|---:|---:|---:|
| base, run 1 | 214.7 ms | over 250 ms | 3,698 | 2,216 | 13.6 s | 1.78 s |
| base, run 2 | 213.4 ms | over 250 ms | 3,173 | 2,184 | 13.7 s | 1.76 s |
| WP-832, run 1 | 183.6 ms | 246.8 ms | 1,380 | 431 | 5.8 s | 1.51 s |
| WP-832, run 2 | 179.0 ms | 241.5 ms | 1,178 | 435 | 5.1 s | 1.26 s |

All 30,000 commands and 120,000 reads completed without retries in every run.
Claim groups cut outbox writer turns by about 5x and the time the outbox holds
the gate by about 60%. Command epochs ran about 25% more often per interval
(311–315 against 246–250), so waits per epoch fell further than the totals
show. p99 dropped below the histogram's 250 ms ceiling in both runs, but every
run still fails the lifecycle `probe_latency` gate (p95 under 100 ms, p99 under
250 ms), as the base does.

Each remaining outbox write holds the gate longer (12–13 ms against 6 ms),
because it claims or completes more events and applies a longer journal suffix.
The rest of the outbox hold time is the per-write journal-suffix drain and
Immediate commit, which ADR-0284 option 3 (journaling outbox transitions with
commands) would address.

The scratch attribution is not part of the product. A committed gate-wait
census belongs with a decision record of its own.
