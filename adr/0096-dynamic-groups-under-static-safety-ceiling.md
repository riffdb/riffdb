# ADR-0096: Dynamic Physical Groups Under a Static Safety Ceiling

- **Status:** Accepted
- **Date:** 2026-08-05
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `PERF-004`, `PERF-005`, `PERF-006`, `PERF-008`,
  `PERF-009`, `PERF-010`
- **Related work packages:** `WP-456`
- **Amends:** ADR-0058, ADR-0059, ADR-0060, ADR-0094, ADR-0095
- **Amended by:** ADR-0097, ADR-0098

## Context

ADR-0095 made conflicting FIFO commands eligible for one transaction-local
serial micro-batch without changing their independent application semantics.
Fresh full-seed evidence now stages 19,220 ordinary public commands in 358
physical commits. The writer spends about 2.45 of 3.9 seconds in those durable
commit boundaries. Most saturated groups reach the existing 64-command count
ceiling while remaining well below the independent 16 MiB staged-write bound.

The count ceiling is therefore a physical amortization limit rather than a
current safety discriminator for this workload. Removing it entirely would be
unsound: even tiny commands retain per-command identity, audit, response,
uncertainty, cancellation, and recovery work. Making the absolute bound a
runtime tuning option would also make worst-case memory and recovery work
configuration-dependent. The design needs one larger static safety ceiling and
a dynamically selected actual group below it.

## Decision

The internal authoritative transaction count ceiling is **256 commands**. The
16 MiB semantic and conservative encoded staged-write ceiling is unchanged.
The public application transport batch remains capped at 16 ordinary commands.

The actual physical group is not fixed at 256. For every formation the
scheduler selects the largest currently eligible contiguous FIFO prefix bounded
by all of:

- commands available before the oldest selected command's existing
  200-microsecond deadline;
- the 256-command static ceiling;
- the unchanged 16 MiB staged-write ceiling;
- same-snapshot compatibility or ADR-0095 serial-path eligibility;
- conflict authority and transaction-current validation;
- request cancellation and deadlines; and
- every existing catalog, capability, administrative, readiness, fencing,
  shutdown, and typed-lane hard barrier.

The scheduler MUST NOT extend or restart the oldest-command deadline merely to
approach 256. Queue depth, traffic, command size, compatibility, byte
reservation, and barriers therefore determine a dynamic effective cardinality
from 1 through 256. Unary traffic remains a group of one unless compatible work
arrives within the already accepted window.

The coordinator's count and retained-byte admission bounds remain independent
and must exceed one maximum physical group. Enlarging the count bound does not
permit an unbounded queue, preparation pool, reorder buffer, conflict set,
response collection, audit allocation, or retry loop.

Every command retains its independent authorization, identity, canonical
input, transaction context, outcome, sequence, mutation graph, event, outbox
intent, provenance, audit lifecycle, response-release proof, acknowledgement,
and uncertainty classification. A pre-commit failure proves the complete
physical group absent. Unknown commit status fences writes and resolves all
selected identities independently before any result is released. Recovery may
not infer one command from another and must classify a 256-command group within
the same fixed count and byte bounds.

Completion telemetry expands to fixed buckets for exact group sizes 1 through
256. It remains bounded and contains no identifier, command name, tenant,
payload, or other high-cardinality label.

## Consequences

- Saturated small-command workloads may amortize one Immediate flush across
  more commands without weaker durability or public transaction semantics.
- Large commands continue to stop at the byte ceiling, often far below 256.
- Worst-case retained command metadata, cancellation checks, audit sequence
  allocation, uncertainty lookups, and recovery classification increase by a
  fixed factor of four and require exact-bound tests.
- Low-load latency and the maximum grouping wait are unchanged.
- The safety ceiling remains a compiled and tested product invariant, not a
  deployment tuning knob.

## Rejected alternatives

- **Always wait for or execute 256 commands.** This would regress unary and
  burst-tail latency and could cross hard barriers.
- **Make the absolute ceiling runtime-configurable.** This makes memory and
  recovery guarantees configuration-dependent and weakens conformance.
- **Remove the count ceiling and rely only on bytes.** Tiny commands still
  consume bounded per-command control, audit, result, and recovery state.
- **Increase the 200-microsecond window.** The observed seed groups are already
  count-bound; a longer wait changes latency policy without addressing the
  static cap.
- **Increase the 16 MiB transaction ceiling.** Current evidence does not show
  that byte bound limiting the representative seed, and changing it enlarges a
  different memory and durable-record risk surface.

## Acceptance reference

The maintainer explicitly approved the static-256/dynamic-effective-group rule
in the current Codex session on 2026-08-05. The approval retained the public
batch limit of 16, the 16 MiB transaction limit, the existing oldest-command
deadline, FIFO and hard barriers, one authoritative writer, Immediate durable
acknowledgement, independent command semantics, and fail-closed uncertainty
recovery.
