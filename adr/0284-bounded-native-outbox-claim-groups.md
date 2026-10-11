---
adr: "0284"
title: Bounded Native Outbox Claim Groups
status: proposed
tier: guarantee
date: 2026-10-11
accepted: null
acceptance: null
requires: [ADR-0273, ADR-0269, ADR-0004]
amends: [ADR-0273]
supersedes: []
requirements: [EFF-002, EFF-003, REC-001, END-008]
packages: [WP-832]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations:
  - id: outbox-claim-group-atomicity
    package: WP-832
    proof: outbox_claim_group_preserves_exact_status_cas_and_atomic_receipt
    description: A checked claim group preserves reciprocal validation, per-event CAS, absent and present preimages and one atomic captured receipt.
  - id: outbox-claim-group-admission
    package: WP-832
    proof: outbox_claim_group_admits_only_what_the_destination_accepts_together
    description: A claim group never contains an event its destination could not accept together with the earlier members, so quota exhaustion defers instead of consuming an attempt.
  - id: outbox-claim-group-crash
    package: WP-832
    proof: outbox_claim_group_crash_redelivers_without_losing_archive_records
    description: Process loss with claimed-but-undelivered group members leaves them Delivering, fenced and recovered by the existing startup normalization.
review_triggers:
  - Claim group size, receipt limits, FIFO writer policy, journal format or selected commit profiles change.
  - Connector I/O occurs under the writer lease, a member is delivered after its lease deadline without renewal, or an event is claimed that its destination would refuse with the group.
  - Renewals, retries, dead letters or archive commits are merged, or acknowledgements precede durable destination acceptance.
---
# ADR-0284: Bounded Native Outbox Claim Groups

## Context

ADR-0273 grouped successful completions but kept one primary transaction per claim, deferring claim groups "until evidence requires
that larger change". The evidence: on the second development machine (8 vCPU, AMD EPYC 7B12), a 300-second observed TicketDesk lifecycle
preparation at 100 commands/s and 400 reads/s completed 95,110 archive acceptances with about one primary outbox transaction per event,
nearly all of them claims. Each held the sole writer gate for about 6 ms, because every direct write first materializes the published
command journal suffix and then commits Immediate. Outbox transactions held the gate about 46% of the load window, and an instrumented
gate attributed 1.5 to 2.1 s of command-epoch waiting per 30 s to them, against at most 0.45 s for every other direct writer: about 7 ms per
command epoch, up to 136 ms for one. Both runs failed `probe_latency` (p95 230 and 218 ms, 4,493 and 3,508 histogram overflows).

ADR-0273 rejected claim groups for three hazards: claims ahead of connector I/O, aggregate quota admission and additional lease exposure.
This record decides how each is bounded.

## Decision

1. Amend ADR-0273 decisions 1 and 2 only to permit one primary Immediate transaction to claim a checked group of at most sixteen pending
   events from one scan page. Completion groups, renewals, retries, dead letters, archive two-phase Immediate per-record commits,
   directory syncs, commit profiles and the journal protocol are unchanged. Delivery remains sequential and no connector I/O occurs
   under the writer lease.
2. A claim group is the longest run of consecutive scanned items, at most sixteen, each of which is eligible, within its attempt limit and
   admitted by the destination together with every earlier member of the run. The first item failing any of these ends the run and is
   handled exactly as today (deferral or dead letter) after the run is delivered. Successes still flush at the bound, at page end and
   before deferral, state change, retry or dead-letter handling.
3. Destination preflight is cumulative: the connector reports how many leading events of a proposed run it can accept together. The
   durable local archive admits a run while its current encoded bytes plus the charges of the run's new records fit the quota; an equal
   existing record charges nothing. A connector without aggregate state reports the leading run that each passes `ready_to_deliver`.
   So quota exhaustion defers an event without consuming an attempt, exactly as an individual claim does today.
4. Every member is claimed with the same claim timestamp and its existing lease deadline. Before connector I/O for a member, the
   dispatcher compares the clock with that member's lease deadline and, if it has passed, renews the lease through the existing
   individual renewal transition before delivering; a renewal that does not apply ends the run and returns the member's observed state.
5. Validate the group before mutation: nonempty, at most sixteen entries, strictly increasing distinct event IDs, every expected state
   absent or pending, existing claim encoding and receipt byte/entry ceilings, with insert preimages for absent rows. Reject invalid input
   before acquiring a writer turn. Repositories without atomic claim groups keep individual claims; a sequence of separate commits must
   never be represented as one atomic group.
6. Admit each group through the sole FIFO writer. Revalidate every reciprocal event/intent/commit and exact status. An ordinary CAS
   mismatch leaves that row unchanged and returns its observed status; matching rows are claimed together in one existing
   OutboxTransition captured V3 receipt with exact preimages. Missing authoritative intent, corruption or a typed storage failure aborts
   the transaction; no match aborts without a receipt. Pending-index membership changes apply only after durable commit. Only applied
   members are delivered, in event order.
7. Until it completes, retries or dead-letters, every claimed member remains Delivering and fences retention. A crash, cancellation, fatal
   error or a state-change exit may leave up to sixteen claimed members undelivered; existing startup recovery normalizes each to its
   retryable pending state with the claimed attempt counted, and immutable archive deduplication permits redelivery. No live claim is
   normalized by a shortcut, and no success is fabricated.
8. Public command, SDK, MCP, authorization, operator configuration and archive inspection behavior are unchanged. Applications gain no
   batch selector, durability opt-out or transaction callback. Document the claim grouping beside the completion delay.

## Options considered

1. **Keep individual claims:** retains about one primary Immediate transaction per event and the measured command-epoch waits.
2. **Claim groups without cumulative admission:** simpler, but quota exhaustion would surface as connector failures that consume
   attempts and can dead-letter events a later pause would have delivered.
3. **Journal outbox transitions with commands:** removes the per-write suffix drain and separate commit, but changes the journal protocol
   and receipt attribution; a larger record, considered only if claim groups leave command waits too high.
4. **Move delivery state to the derived sidecar:** conflicts with its authoritative, replicated classification (ADR-0004, ADR-0022).

## Consequences

- A full page of sixteen deliveries uses one primary claim commit and one completion commit instead of seventeen; gains depend on page
  occupancy, and no latency claim follows from the count.
- A member may wait for up to fifteen earlier deliveries between its claim and its connector I/O; its lease is renewed if that wait
  exceeds it. Claimed-but-undelivered members after process loss count one extra attempt each against the existing limit of 32.
- Deferred: journaling outbox transitions with commands (option 3).

## Standing design tests

- **Interface safety:** all application writes remain compiled commands; the new port accepts only checked claim transitions below the
  application boundary and exposes no arbitrary mutation, selector or durability choice.
- **Scale:** one dispatcher and writer, the existing scan page and byte ceilings, at most sixteen claims and bounded receipts and results;
  no population-sized state.

## Checks

- The three obligations above, plus a dispatcher test that sixteen pending deliveries issue one claim and one completion commit with the
  same outcomes as individual claims, and that a run ends at the first ineligible, exhausted or unadmitted item.
- A lease-expiry test: a member reached after its deadline is renewed before delivery, and a renewal state change ends the run.
- The ADR-0273 proofs remain unchanged and passing; scoped acceptance and full CI pass before repeating the observed 300-second
  lifecycle preparation with the gate attribution.
