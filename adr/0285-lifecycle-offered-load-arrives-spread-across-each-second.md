---
adr: "0285"
title: Lifecycle Offered Load Arrives Spread Across Each Second
status: proposed
tier: surface
date: 2026-10-11
accepted: null
acceptance: null
requires: [ADR-0265]
amends: []
supersedes: []
requirements: [END-007]
packages: []
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - The probe offers more or fewer than 100 commands and 400 reads in any second, or measures latency from anything but each operation's own offer.
  - The arrival schedule stops being reproducible from the second alone.
---
# ADR-0285: Lifecycle Offered Load Arrives Spread Across Each Second

## Context

ADR-0265 decision 12 offers 100 commands/s and 400 named reads/s with at most 32 in flight, and the lifecycle preparation refuses a normal
window whose p95 is not below 100 ms or whose p99 is not below 250 ms. It does not say how arrivals fall within a second. The probe offered
all 500 of a second's operations at one instant and measured each from that instant, so 28 workers drained a burst every second and the
measured latency was mostly queueing in the probe. On the second development machine (8 vCPU, AMD EPYC 7B12) a 300-second preparation
measured queue wait p50 39 ms and p95 161 ms before any call began, while reads took 1.7 ms on average. The same server with the second's
offers spread across it measured p95 17.7 ms and p99 57 ms.

## Decision

1. Each second offers exactly 100 commands and 400 reads, each at its own offset within that second. Offsets are independent and uniform,
   drawn from a fixed-seed generator keyed by the second, so every run offers the identical schedule. This is a Poisson arrival process
   conditioned on the second's count; no second offers all of its operations at one instant.
2. Each operation's latency is measured from its own offer time, and it counts toward the 300-second window it was offered in. A window is
   published once all of its offered operations settle, or 60 seconds after it closes with what completed; completions after that are
   disclosed as post-duration work, as before.
3. Rates, windows, the in-flight bound, auxiliary slots, retry accounting and the p95 and p99 limits are unchanged. Results taken before this
   record measured the per-second burst and are not comparable to later ones.

## Options considered

1. **Keep the per-second burst:** a spike test of the probe's own 28-worker queue rather than the stated rate; it would need about twice
   the writer throughput per command to pass, without that being the milestone's target.
2. **Even spacing:** simpler, but offers never cluster, so it understates queueing that real traffic produces.
3. **Unseeded random arrivals:** realistic, but two runs would offer different schedules and could not be compared.

## Consequences

- The latency gate measures the server at the stated rate; clustered arrivals still queue, so it is not the most favourable shape.
- No separate burst test gates qualification; a spike diagnostic, if wanted, is a separate report-only probe.

## Standing design tests

- **Interface safety:** nothing on an application, operator or public surface changes; only the preparation's load generator and its
  observation windows.
- **Scale:** the schedule is 500 entries per second, generated per second; window state stays two fixed histograms.

## Checks

- `each_second_offers_exactly_its_operations_spread_across_the_second` holds the counts, ordinals, ordering, spread across the second and
  reproducibility of the schedule.
- `operations_count_toward_the_window_they_were_offered_in` holds offer-window attribution, settle-or-deadline publication and late
  disclosure.
- The required-host preparation's unchanged smoke checks gate rate, latency, concurrency and drain.
