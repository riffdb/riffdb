# ADR-0146: Fixed-Generation Unary Sampling and Whole-Cell Cloud Host Validity

- **Status:** Accepted
- **Direction approved:** 2026-08-24 (maintainer)
- **Exact text accepted:** Yes, 2026-08-24 (maintainer)
- **Evidence ownership split accepted:** 2026-09-03 (maintainer, in session)
- **Decision deadline:** Before WP-674 replaces either retained failed cloud
  attempt or banks any unary baseline
- **Requires:** ADR-0123, ADR-0142, ADR-0143
- **Amends:** ADR-0143 Decisions 1 and 4, `PERF-008`, `PERF-018`, and WP-674's
  fixed sample and host-validity evidence

The maintainer approved this direction and accepted this exact text on
2026-08-24 after the first exact ADR-0143 WP-674 attempts showed that 100
measured operations do not reliably separate product latency from ordinary
cloud scheduling variation.

## Context

ADR-0143 correctly fixed five process generations, retained every extreme,
qualified the median generation, applied an unchanged 20-percent stability
limit to the central three order statistics, and prohibited
performance-selected retries. The first exact portable-artifact N1 and E2 runs
obeyed that rule and stopped at `point_get_ticket`:

- N1 RiffDB qualified p50/p95 were 0.616/0.755 ms and E2 RiffDB qualified
  p50/p95 were 1.656/2.305 ms, all comfortably inside ADR-0142's 3/6 ms
  ordinary-read service levels;
- N1 failed only safe-application PostgreSQL p95 stability, at approximately
  1.239 central-three spread; and
- E2 failed several backend/statistic stability checks while remaining an
  otherwise complete and correctness-clean cell.

Those unfavorable attempts are immutable failed evidence and may not be
retried under ADR-0143. A separate diagnostic could not run the proposed
larger sample because the frozen ordinary benchmark path correctly capped
measurements at 100. The measurement rule therefore needs an accepted
successor rather than an environment override.

The investigation also exposed a normative conflict. `PERF-018` and accepted
ADR-0143 require five generations and central-three stability, while
`PERF-008` still contains ADR-0142's superseded three-generation,
largest-to-smallest wording. The successor must reconcile the authoritative
requirement instead of teaching the verifier to choose one side.

Preflight and postflight process inventories cannot prove that a cloud VM was
not descheduled during the measured cell. Linux exposes bounded cumulative
guest steal accounting in `/proc/stat`; binding its delta across the complete
cell makes that failure independently observable without inspecting the
latency distribution.

## Proposed Decision

### 1. Every unary generation measures exactly 1,000 operations

Each backend, scenario, and host cell still executes exactly five independent
counterbalanced process generations. Every generation runs exactly 20
same-scenario warmups followed by exactly 1,000 measured operations over the
frozen full dataset. All five p50 and p95 values remain mandatory evidence;
the qualified statistic remains the median of five; and the unchanged
central-three rule remains `x4 / x2 <= 1.20`.

The 1,000-operation cardinality is part of the release protocol, not a caller
knob. The ordinary app-baseline surface retains its existing 100-operation
ceiling. Only the fixed WP-674 receipt mode may request 1,000 operations, and
that mode rejects every other sample, warmup, or generation cardinality.

Increasing samples does not authorize a retry of either failed 100-operation
attempt. It creates a new measurement-governance identity and restarts each
profile from its first scenario using one exact artifact.

### 2. Host validity binds whole-cell CPU steal

The host-validity preflight records bounded cumulative total CPU and steal
ticks from the aggregate Linux `/proc/stat` CPU row. The postflight binds the
exact preflight observation and records the nonnegative whole-cell deltas.
Evidence is host-invalid when:

- the preflight is absent, oversized, linked, malformed, or belongs to a
  different host identity or boot;
- counters regress, the logical-CPU count changes, or total elapsed CPU ticks
  are zero;
- aggregate steal exceeds 1.00 percent of aggregate elapsed CPU ticks; or
- either existing bounded process/IO inventory rejects the host.

The 1.00-percent ceiling is fixed before the replacement measurements. It may
not be raised or disabled from the WP-674 runner, environment, profile, or
report. A host-invalid attempt remains receipted and ADR-0143's one-replacement
limit remains unchanged. A completed latency distribution cannot be used to
infer, edit, or reclassify steal.

The host-validity V1 schema and all receipts containing it remain immutable.
New benchmark evidence uses V2, which adds bounded raw counter observations,
a boot-bound preflight identity, the fixed threshold, deltas, and the computed
whole-cell percentage. V2 contains no process arguments, credentials, or
unbounded `/proc` text. Consumers may continue to verify historical V1
receipts, but WP-674 qualification requires V2 at both boundaries and a valid
whole-cell result.

### 3. Stability, extremes, and retry governance do not change

ADR-0143's five-generation median, central-three 20-percent spread, mandatory
extreme retention, counterbalanced order, and performance-selected retry ban
remain exact. A valid 1,000-operation cell that still exceeds the fixed spread
fails WP-674. It does not trigger another sample increase, a wider threshold,
generation deletion, host reclassification, or a retry.

Safe-application and minimal PostgreSQL remain mandatory disclosure evidence.
No result may be selected, paired, weighted, or excluded based on favorable or
unfavorable latency.

### 4. New WP-674 receipts cannot be confused with prior attempts

WP-674 profile fragments, the assembled manifest, the frozen baseline, and
qualification receipts advance to V2 and bind:

- the exact source revision, `Cargo.lock`, harness, runner, and server
  artifacts;
- the fixed `5 x (20 + 1,000)` unary measurement shape;
- every retained generation and extreme;
- both V2 host-validity observations and their whole-cell steal result;
- the unchanged scenario, backend, profile, service-level, regression, and
  correctness identities; and
- immutable references to earlier failed governance attempts rather than
  rewriting or promoting them.

V1 WP-674 schemas remain verifiable historical evidence but cannot satisfy the
new qualification. The app-baseline workload/result schema changes only by
additive bounded host evidence and the already reported measured-operation
cardinality; application operations, transport, semantics, and product bytes
do not change.

### 5. `PERF-008` and `PERF-018` state one rule

`PERF-008`'s superseded three-generation paragraph is replaced by the accepted
five-generation rule, 20 warmups, 1,000 measurements, median of five, and
central-three stability. `PERF-018` receives the same sample cardinality and
requires whole-cell V2 host validity. The scenario classes, absolute ceilings,
low-water regression ceiling, PostgreSQL disclosure, mixed and seed gates,
and every semantic freeze remain unchanged.

## Options Considered

1. **Retry the failed 100-operation cells:** rejected as explicitly
   performance-selected under ADR-0143.
2. **Raise the 20-percent spread:** rejected because the threshold would be
   tuned after observing unfavorable results and would weaken bimodality
   detection.
3. **Use 1,000 operations with the unchanged five-generation rule:** proposed.
   It increases within-generation evidence without hiding any generation.
4. **Use time-based samples:** rejected because host speed would change sample
   cardinality and percentile resolution across profiles.
5. **Treat CPU steal as diagnostic only:** rejected because cloud descheduling
   is an independent host-validity failure that can bias both systems and must
   be known before qualification.
6. **Allow a profile-specific steal ceiling:** rejected because it creates a
   caller-selectable evidence standard and encourages post-result tuning.

## Consequences

- Each unary generation is ten times longer while the total matrix remains
  finite: three profiles, fourteen scenarios, two required comparator shapes,
  five generations, 20 warmups, and 1,000 measured operations.
- Longer cells reduce percentile quantization and short scheduler-burst
  sensitivity, but they deliberately increase the chance that sustained host
  interference is detected.
- Whole-cell steal makes a class of cloud interference fail before latency
  qualification without allowing latency to choose the host result.
- A still-unstable valid cell stops the campaign and requires investigation or
  a separately accepted governance decision.

## Compatibility

This decision changes no application API, command or query, generated binding,
transport, authorization, row or field policy, boundedness, freshness,
durability, uncertainty, storage format, recovery behavior, workload,
comparator obligation, scenario class, or latency ceiling.

Historical host-validity and WP-674 V1 evidence remains byte-exact and
verifiable. New qualification emits V2 receipts and cannot silently consume or
upgrade V1. The ordinary benchmark keeps its prior caller-facing sample bound;
only the fixed release-evidence path admits the larger cardinality.

## Security

The runner, not the caller, owns sample cardinality and the steal ceiling.
Reports contain bounded counters and hardware/boot identity only; they contain
no command lines, environment, credentials, payloads, or unbounded kernel
content. Counter parsing fails closed. Existing redaction, authorization,
policy, correctness, and response-release paths execute unchanged.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** no application interface or
  guarantee changes. An application cannot select samples, generations,
  stability arithmetic, host classification, comparator, baseline, or retry.
- **Scale:** every dimension and evidence field is fixed and bounded. Raw CPU
  observations are constant-size; no per-operation, history-sized, or
  database-sized evidence is retained.
- **Pay once:** CPU counters are sampled once at each cell boundary and proven
  for the cell, never re-read per operation.

## Testing

- Unit fixtures prove `/proc/stat` parsing, zero and regressing counter
  rejection, boot/host mismatch rejection, exact 1.00-percent boundary
  behavior, and process-argument redaction.
- Harness tests reject every WP-674 sample cardinality except 1,000, while the
  ordinary benchmark still rejects caller-selected values above 100.
- V2 schema tests reject V1 qualification, missing or extra generations,
  missing extremes, missing or unbound preflight, invalid whole-cell steal,
  host drift, and all existing semantic/correctness/identity failures.
- Positive fixtures retain low and high extremes around a stable central three
  and accept steal at or below the fixed ceiling.
- The complete N1, E2, and workstation bank starts from scenario one. If any
  independently host-valid cell remains unstable, WP-674 stops without retry
  or threshold change.

## Requirements and Work Packages

- **Requirements:** `PERF-008`, `PERF-018`, `END-007`, `END-008`
- **Endurance evidence:** `WP-578` separately and solely owns `END-009`,
  `END-010`, the uninterrupted 72-hour run, and the final endurance receipt.
- **Defines or blocks:** `WP-674`, `WP-623`, `WP-579`
- **Final unary qualification evidence:** `WP-674`.

## Decision Deadline

The exact text must be accepted before `SPEC.md`, V2 evidence schemas, host
validity, WP-674 runners/verifiers, handbook text, or replacement measurements
change.
