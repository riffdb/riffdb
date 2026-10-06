---
adr: "0272"
title: Source Progress Aware Maintenance Rechecks
status: accepted
tier: guarantee
date: 2026-10-05
accepted: '2026-10-05'
acceptance: 'maintainer, in session, 2026-10-05: Approved, in response to exact-text ADR-0272 review (version b1b52eeb)'
requires: [ADR-0182, ADR-0268, ADR-0271]
amends: [ADR-0268, ADR-0271]
supersedes: []
requirements: [STO-030, STO-031, END-008]
packages: [WP-825]
obligations:
  - id: source-progress-recheck
    owner: WP-825
    package: WP-825
    proof: completed_source_deletion_is_progress_even_when_logical_retention_is_idle
    description: Actual source deletion receives progress rechecks while an idle source keeps its bounded idle backoff.
  - id: source-progress-rest
    owner: WP-825
    package: WP-825
    proof: source_progress_rechecks_preserve_gate_rest_and_idle_backoff
    description: Progress removes only the fixed idle delay, preserving actual writer-residence rest and cancellation.
  - id: source-progress-release-gates
    package: WP-825
    proof: scripts/check-ticketdesk-lifecycle
    sources: [ebe331e139748003e8176cf1cbcc0f758f9a14d5fa81a54745c8dd0e6cf13b98]
    says: Validate the frozen measured rehearsal contract; real scoped CI and unchanged host preparation remain the owning package exit gate.
review_triggers:
  - An idle or refused source bypasses backoff or a planning ceiling substitutes for actual deletion.
  - Actual writer-residence rest, source admission, independent checkpoint licensing, deletion bounds or any current hold changes.
  - Public controls, durable formats, command ordering, acknowledgement or retry semantics change.
---
# ADR-0272: Source Progress Aware Maintenance Rechecks

## Context

The source admission repair passes full CI on `1841f1f1`. An isolated
production-policy regression reproduces an additional native limitation:
a pass that actually deletes source receipts still selects a one-second idle
recheck when logical retention is at its target or held. The source validator
is complete, so its pending-validation flag is correctly false. With at most
256 receipts deleted per pass, this branch limits deletion to at most 256
receipts per second even before writer-residence rest. The selected workload
produces one command receipt and two durable outbox transition receipts per
successfully delivered update, or 300 receipts per second at the offered rate.
Fair admission cannot remove this scheduling ceiling. The bound alone does
not identify every cause of measured latency or guarantee adequate throughput.

ADR-0268 decision 1 and ADR-0271 decision 8 preserve existing maintenance
pacing, and ADR-0271 explicitly requires review when it changes. Broad native
repair authorization therefore does not accept this exact refinement. The
record remains proposed until the maintainer accepts its text. No fixed-delay
removal or faster host workload is authorized by this proposal alone.

## Decision

1. Amend only the fixed idle recheck classification in ADR-0268 decision 1
   and ADR-0271 decision 8. When the native step reports a positive count of
   source receipts durably removed during that exact pass, use the existing
   progress recheck policy even if its separate logical retention outcome is
   TargetReached, RowBoundary or Fenced. Zero removal keeps the current idle
   recheck, except for the already authorized pending-validation case.
2. Apply the existing writer-residence rest after every attempt without
   change: charge all actual gate-held work, including drain, validation,
   deletion, durable publication and failure cleanup, through the existing
   factor and cap. Preserve checkpoint waits, cancellation, ordinary writer
   admission, the single source owner and the existing four auxiliary lanes.
   A progress recheck grants no priority, parallel writer or extra quantum.
3. Use only the native pass's actual committed deletion count. Do not treat
   a planning ceiling, eligible range, queued request, completed validation,
   checkpoint observation or inferred byte count as source progress. Do not
   manufacture a pending-validation flag to select the progress branch.
4. Preserve every source validator, qualification binding, hold, independent
   checkpoint and prior-observation rule. Keep the 256-receipt deletion bound,
   bounded FIFO wait and bounded complete journal drain. No source receipt
   may license its own next deletion. Preserve off-gate retirement and current
   validated-prefix refresh, durability profiles, fsyncs and journal protocol.
5. Keep public health labels, command semantics, authorization, ordering,
   outcome recovery, durable formats and operator configuration unchanged.
   Add no public pacing, force, bypass, retry or load-reduction control.
6. Prove actual-progress rechecks, zero-progress idle backoff, unchanged
   full-residence rest and shutdown cancellation deterministically. Run scoped
   acceptance and full CI, then repeat the unchanged required-host workload.
   Accept no rehearsal from synthetic counters or a faster fixed policy alone;
   normal-window latency, actual reclamation and all milestone gates remain
   required. No 24-hour run starts as part of this repair.

## Options considered

1. **Keep the fixed delay after deletion:** preserves the current policy but
   retains a scheduling ceiling below the selected workload's receipt rate.
2. **Increase deletion size or reduce load:** changes independent bounds or
   the qualification target and avoids proving the requested workload.
3. **Recognize actual source progress:** selected; preserves gate rest,
   deletion authority and idle backoff while removing the unrelated fixed delay.

## Consequences

- Source progress can request a subsequent bounded turn after its required rest.
- More frequent attempts may still fail latency or fall behind; host evidence
  decides qualification. No sustained-throughput or filesystem deadline is added.
- Zero-progress passes retain backoff, avoiding an idle maintenance feedback loop.
- Byte-accounted eligible history, complete campaign coverage and any further
  durability or performance changes remain separate work.

## Standing design tests

- **Interface safety:** applications gain no unsafe operation or guarantee
  opt-out; commands, idempotency, scoping and acknowledged durability remain.
- **Scale:** one owner, fixed admission capacity, bounded validation/deletion
  and exact gate-rest accounting remain. Progress is actual durable work,
  never population-sized diagnostics or a fabricated eligible-history count.

## Checks

- The production idle-policy regression fails on `1841f1f1` with one second
  instead of a progress recheck after actual positive deletion phases.
- The two named obligations prove progress, idle backoff, rest and cancellation.
- Existing source admission, hold, corruption, independent-checkpoint,
  no-self-stimulation and native process-crash tests remain required.
- `scripts/acceptance --wp WP-825`, `scripts/ci-all` and unchanged host
  preparation must pass before this component can support milestone closure.
