# WP-449 writer evidence

Status: diagnostic instrumentation implemented; PostgreSQL parity remains an
unchanged `PERF-008` alpha gate.

## What the benchmark now proves

The default per-level RiffDB load path uses a fresh identical database for each
client point and restarts `riffdbd` after bootstrap, deploy, and seed. The
process-generation telemetry therefore covers only that point's warmup and
measurement. The report records:

- read-only, write-only, and mixed public application profiles;
- logical command attempts and successful mutations per second;
- writer busy/idle time and accepted queue delay;
- intake selection/defer counts;
- compatibility groups and split cause (`conflict_key` or `exact_access`);
- physical commit/flush duration and commands per physical commit; and
- process writes and durable growth per successful mutation.

All labels are closed numeric dimensions. No application value, key, tenant,
principal, command name, or submitted text enters the evidence line.

## 2026-08-05 diagnostic result

These short runs are causal diagnostics, not release evidence. The smoke
write-only run used 32 clients, uniform ticket selection, a one-second warmup,
and a two-second measured window:

| Metric | Result |
|---|---:|
| successful commands | 1,952 |
| measured throughput | 978 commands/s |
| writer busy time | 3.007 s |
| writer idle time | 0.016 s |
| commands presented to compatibility | 2,946 |
| physical compatible groups | 779 |
| conflict-key splits | 594 |
| exact-access splits | 0 |
| commands per physical commit | 3.78 |
| durable commit/flush time | 2.631 s |

A five-second full-data uniform write-only probe improved group fill to 13.15
commands per physical commit and delivered 2,101 commands/s, but each physical
commit still averaged about 4.79 ms. It wrote approximately 53,794 kernel bytes
and grew durable files by approximately 13,602 bytes per successful mutation.

Those per-mutation byte figures used measured-window successes as the
denominator while the daemon counters also included warmup. WP-452 corrects
the report to normalize process-scope bytes by every command committed by that
daemon generation. Keep the WP-449 values only as historical evidence; do not
compare them directly with corrected WP-452 output.

The evidence rejects two earlier guesses:

- the writer was not under-fed; it was busy for effectively the complete
  warmup-plus-measure process interval; and
- exact entity read/write overlap was not causing the observed group cuts in
  the uniform smoke probe. Every measured compatibility cut was caused by a
  declared aggregate conflict key.

## Safety boundary for the next change

The current aggregate conflict key is not incidental locking. It is the proof
that commands sharing an invariant domain execute in an allowed serial order.
Simply ignoring duplicate keys, allowing later disjoint work to overtake FIFO,
or making visible non-durable commits would conflict with accepted ADRs and is
not an optimization.

The next implementation must therefore choose, with an ADR amendment and
adversarial semantic tests, between:

1. a compiler-proven commutative child-append class that may share one retained
   aggregate exclusion lease only when no cross-command invariant or range
   dependency can be invalidated; or
2. a bounded transaction-local serial micro-batch that evaluates conflicting
   commands in ingress order, retains per-command outcomes and sequences, and
   releases no acknowledgement or partial state before one durable commit.

Independently, WP-452 should attribute the roughly one-page-per-authoritative-
table physical write floor. Any table co-location or durable-key change is a
format decision and requires crash, reopen, migration, and compatibility
evidence; required outcomes, events, provenance, audit, outbox intent, and
idempotency state may not be dropped.
