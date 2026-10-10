---
adr: "0283"
title: Validation Walks Decode Each Command Segment Once
status: accepted
tier: guarantee
date: 2026-10-10
accepted: '2026-10-10'
acceptance: 'maintainer, in session, 2026-10-10: "i accept", in response to the exact-text acceptance request for ADR-0283 as committed at 990243098'
requires: [ADR-0182, ADR-0203, ADR-0263]
amends: []
supersedes: []
requirements: [REC-007, REC-008]
packages: [WP-831]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - A segment decode can succeed with a segment other than the one its row bytes name, through the reading transaction, the clock or a resolver.
  - A remembered segment is served for a lookup whose row bytes differ from the bytes it was decoded from.
  - A refused decode is remembered, or a lookup reports a different finding, error or member than a lookup without reuse.
---
# ADR-0283: Validation Walks Decode Each Command Segment Once

## Context

The complete structural walk (ADR-0182 Decision 3, run by the background scrub and by complete startup validation) cross-checks every
service-audit row's command link. Each check resolves the linked command with `command_member_at`, which decodes the whole `COMMITS` segment
holding it, resolving a history stub from its journal extent first (ADR-0263). The link check resolves the same command for its commit side
and again for its provenance side, and the audit, request-index and lifecycle phases each check the same record, so one segment of N
commands was decoded several times per command. In a write-only TicketDesk profile on the second development machine (8 vCPU, AMD EPYC
7B12) the post-start scrub was 31% of daemon CPU samples and took 30.7 s; 70% of the scrub's samples were these repeated decodes. With the
reuse below, the same scrub takes 9.3 s instead of 19.8 s (median of six alternating runs, the harness waiting for the scrub before load).

Separately, the scrub's reported row count added the session's running total after every page, so `background_scrub_rows` and
`background_scrub_rows_per_second`, which REC-008 requires release evidence to report as scrub throughput, overstated it by roughly the
number of pages.

## Decision

1. A complete structural walk may remember the last command segment it decoded together with the exact `COMMITS` row bytes it decoded it
   from, and reuse it for a later lookup whose row bytes are byte-for-byte equal. Any other row is decoded from its own bytes and replaces
   the remembered segment. A remembered segment is never served for different bytes.
2. Only a successful decode is remembered. A row that is refused is decoded again, and refused again, on every lookup, so every finding,
   error and member a walk reports is the one it reports without reuse, except that a read failure of a history stub's journal extent on
   a second resolution of a frame the walk has already read and verified is not observed.
3. Reuse is held by one walk session and by the bounded sampled checkpoint pass, holds one segment, and is dropped with them. Ordinary
   reads, writers and recovery replay keep decoding the row they read.
4. The scrub's row observation is the session's running total, recorded after each page, not a sum of running totals.

Equal bytes imply an equal decode: an inline segment is decoded from its bytes alone, and a history stub names its frame by hash, so a
successful resolution can only yield that frame's segment (ADR-0263 Amendment 3). The walk's extent pin keeps the frame from being reclaimed
while it runs. Every row and frame is still read and verified at least once; what reuse skips is re-reading bytes already verified.

## Options considered

1. **Keep decoding per lookup:** rejected; the repeated work dominates the scrub and complete startup validation without checking anything
   more.
2. **Route link checks through the walk's command-history window:** rejected; that window decodes rows with a different decoder and maps a
   misplaced segment to an error where the link check reports a finding, so it would change outcomes.
3. **Cache decoded segments by key or transaction:** rejected; the key alone does not identify the bytes across snapshots, and a larger
   cache adds memory proportional to history for no gain over one segment in commit order.

## Consequences

- One decode per segment for the link checks of a walk that visits a segment's commands together; the checks themselves are unchanged.
- The walk retains at most one decoded segment and its row bytes beyond what it retained before.
- `background_scrub_rows` and `background_scrub_rows_per_second` report the true structural row count; evidence banked before this decision
  overstates them.
- Deferred: the scrub's remaining duplicate work (a second SHA-256 over each resolved frame, and re-encoding each rebuilt receipt only to
  decode it) is not changed here.

## Standing design tests

- **Interface safety:** nothing on a public surface changes; no caller can select reuse, and the scrub's health states, rate bound and
  cancellation are untouched.
- **Scale:** constant memory per walk, one segment; decodes fall from several per command to one per segment.

## Checks

- `service_link_checks_decode_a_shared_command_segment_once`: over a 16-command segment, the 32 link checks sharing one walk's reuse decode it
  once (with reuse disabled, its 64 checks decode it 128 times); every intact link validates and every link naming another command's
  provenance is refused, with and without reuse.
- `segment_reuse_follows_the_row_bytes_and_never_remembers_a_refusal`: a remembered lookup returns the same member as a direct one; a later
  snapshot with different row bytes is decoded from its own bytes and returns the direct lookup's refusal on every attempt; the earlier
  snapshot is still read from its own bytes.
- `scrub_row_observation_is_the_running_total_not_a_sum_of_totals`, and the unchanged ADR-0182 and ADR-0203 scrub proofs, including
  `background_scrub_health_transitions_are_closed_and_cancellable`.
