# ADR-0143: Fixed-Generation Unary Stability Without Performance-Selected Retries

- **Status:** Accepted
- **Direction approved:** 2026-08-23 (maintainer)
- **Exact text accepted:** Yes, 2026-08-24 (maintainer)
- **Decision deadline:** Before WP-674 reruns or banks the alpha unary baseline
- **Requires:** ADR-0123, ADR-0142
- **Amends:** ADR-0142 Decisions 2 through 4 and WP-674's repetition-stability
  rule

The maintainer approved this direction on 2026-08-23 and accepted this exact
text on 2026-08-24 after the first exact
WP-674 cloud run proved that ADR-0142's three-generation extreme-spread rule
cannot distinguish a stable product from ordinary shared-cloud scheduling
noise.

## Context

ADR-0142 correctly replaced an unreachable universal PostgreSQL unary ratio
with absolute cloud service levels, a frozen RiffDB regression bank, and full
same-run PostgreSQL disclosure. Its first implementation used three independent
process generations and invalidated a scenario when the largest and smallest
generation statistic differed by more than 20 percent. The runner then allowed
three complete attempts to find a valid set.

The first N1 and E2 runs failed on `point_get_ticket` before reaching any other
scenario even though every observed RiffDB latency remained far below the
absolute service level. N1 attempt three had tightly grouped p50 values but one
p95 outlier; E2 produced a different isolated extreme on each attempt. The
hosts passed their validity inventories. The failures therefore demonstrate a
measurement-governance defect: with only three observations, one ordinary
cloud interruption is both an extreme and one third of the sample, while
repeating the whole cell until all extremes happen to fit is
performance-selected retry.

The evidence must remain fixed, bounded, unfavorable-result preserving, and
strict enough to reject a genuinely unstable release. It must not require an
uninterrupted cloud scheduler across every process generation.

## Proposed Decision

### 1. Every unary cell has five fixed process generations

Each backend, scenario, and host cell executes exactly five independent process
generations. Every generation retains ADR-0142's 20 same-scenario warmups and
100 measured operations over the frozen dataset. The counterbalanced backend
order, process isolation, host inventory, exact artifact identity, correctness,
request/response accounting, and all other `PERF-018` freezes remain unchanged.

All five per-generation p50 and p95 values are retained in the report. The
qualified p50 and p95 are the medians of the five corresponding values. No
generation may be deleted, replaced, or rerun because its latency is high or
low.

### 2. Stability is proved by the central three order statistics

For one backend and one statistic, sort the five per-generation values as
`x1 <= x2 <= x3 <= x4 <= x5`. The statistic is stable exactly when
`x4 / x2 <= 1.20`; `x3` is its qualified value. This rule is applied
independently to RiffDB p50, RiffDB p95, safe-application PostgreSQL p50, and
safe-application PostgreSQL p95 for every scenario and host.

The two extremes remain mandatory disclosure evidence but do not independently
invalidate an otherwise tight central cluster. A cell is invalid if any one of
its four central-three spreads exceeds 20 percent. A missing generation,
non-finite or non-positive statistic, correctness difference, semantic drift,
identity mismatch, or host-invalid observation also invalidates the cell.

RiffDB/PostgreSQL ratios are computed from the two qualified backend medians and
reported. They are derived disclosure values, not a second stability test over
per-generation ratios; counterbalanced generations are not paired observations
and a ratio-of-extremes rule would reintroduce the same noise amplification.

### 3. Performance-selected retries are prohibited

There is one fixed measurement set per exact artifact and valid host attempt.
An unstable completed cell is retained as failed evidence and is not retried to
seek a passing sample. Investigation may produce a new exact artifact or an
accepted measurement-governance amendment, either of which receives a new
receipt identity.

A whole-host attempt may be replaced only when a predeclared host-validity,
artifact-identity, topology, correctness, or infrastructure-completion check
fails independently of measured performance. The invalid attempt and its
reason remain in the receipt. At most one replacement host attempt is allowed;
a second host-invalid attempt fails the run. A completed latency cell cannot be
reclassified as host-invalid from its result distribution.

### 4. Baseline and release arithmetic are otherwise unchanged

ADR-0142's scenario classes, N1/E2 absolute ceilings, workstation regression
obligation, byte-exact baseline identity, 1.10-times RiffDB regression ceiling,
PostgreSQL disclosure, mixed-load gates, seed ceiling, low-water-mark rules,
and requirement ownership remain unchanged. WP-674 must restart the unary bank
from the first scenario under this five-generation method; its prior
three-generation attempts remain historical failed evidence and cannot be
merged into the accepted bank.

## Options Considered

1. **Keep three generations and three retries:** rejected because it rewards
   repeated sampling until cloud extremes align and still fails stable product
   cells unpredictably.
2. **Use five generations and require all-five extreme spread:** rejected
   because one scheduler interruption still invalidates the entire cell while
   contributing no evidence about the typical warmed process.
3. **Use five generations and central-three spread:** proposed. It requires a
   majority cluster, retains every extreme, and prevents one low plus one high
   outlier from controlling the release decision.
4. **Remove the stability rule:** rejected. A median without a dispersion check
   can hide bimodal service behavior or a broken host.
5. **Raise the spread percentage until three generations pass:** rejected as a
   post-result threshold change with weaker detection and no principled bound.

## Consequences

- Each unary cell costs five process generations instead of three, increasing
  runtime by two thirds while remaining finite and much smaller than the final
  endurance matrix.
- A majority of generations must agree within 20 percent for both systems and
  both percentiles; bimodal or persistently noisy behavior still fails closed.
- One low and one high cloud interruption remain visible without forcing a
  product rebuild or encouraging repeated measurement.
- Existing ADR-0142 absolute and regression gates are not loosened.

## Compatibility

This decision changes no public API, application operation, compiler output,
transport, authorization, durable format, recovery behavior, workload,
comparator, scenario class, or latency ceiling. It changes only the bounded
repetition and stability arithmetic in benchmark and release evidence.

The original three-generation WP-674 artifacts remain immutable failed
receipts under ADR-0142. They are not rewritten or promoted.

## Security

No application or benchmark caller can select generations, discard extremes,
change the central cluster, weaken a service level, or request a retry. Reports
remain value-free and credential-free. Authorization, policy, boundedness,
freshness, durability, uncertainty, recovery, and response release execute
unchanged in every measured operation.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** this decision adds no
  application interface and no safety opt-out. Evidence invalidates before
  qualification on semantic, correctness, identity, or host drift.
- **Scale:** five generations, 20 warmups, 100 measurements, three profiles,
  and the frozen scenario registry are fixed bounds. No database-size-dependent
  memory, full-state cache, or co-located authority is introduced.

## Testing

- Report-schema tests freeze five-generation cardinality, median-of-five
  selection, central-three spread arithmetic, and ratio-of-qualified-medians.
- Negative fixtures reject missing or extra generations, a central-three spread
  above 20 percent, performance-selected replacement, a second host attempt,
  post-result host invalidation, missing retained extremes, and all existing
  ADR-0142 identity, semantic, correctness, and classification failures.
- Positive fixtures cover one low outlier, one high outlier, and simultaneous
  low/high extremes around a stable central three.
- WP-674 runs the complete N1, E2, and workstation bank from generation one and
  receipts the prior failed ADR-0142 attempts separately.

## Requirements and Work Packages

- **Requirements:** `PERF-008`, `PERF-018`, `END-007`, `END-008`, `END-009`,
  `END-010`
- **Defines or blocks:** `WP-674`, `WP-623`, `WP-579`
- **Final evidence:** `WP-674`

## Decision Deadline

Exact acceptance is required before WP-674 changes its runner, verifier, report
schema, fixtures, or release baseline and before any new unary measurements are
used for qualification.
