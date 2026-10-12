---
adr: "0286"
title: Validation Walks Validate Each Contract Bundle Once
status: accepted
tier: guarantee
date: 2026-10-11
accepted: '2026-10-11'
acceptance: 'maintainer, in session, 2026-10-11: "i accept", in response to the exact-text acceptance request for ADR-0286 as committed at e4d3d4836'
requires: [ADR-0283, ADR-0182, ADR-0203]
amends: []
supersedes: []
requirements: [REC-007]
packages: [WP-833]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - A remembered contract bundle is served for a stored record whose identity or canonical bytes differ from the record it was validated from.
  - A remembered segment is served for row bytes that differ from the bytes it was decoded from, or either memo remembers a refusal.
  - A lookup through either memo reports a different finding, error or member than the same lookup without it.
---
# ADR-0286: Validation Walks Validate Each Contract Bundle Once

## Context

The complete structural walk (ADR-0182 Decision 3, run by the background scrub and by complete startup validation) inspects every `COMMITS`
row. For each row it validates the commands' supplied index images against their contract bundle, and it then resolves the same commands
through the walk's command history. Two costs repeat per row although their inputs do not change. The image check reads the command's stored
contract bundle and fully decodes and validates it (IR decode, command registry, generated schema artifacts, plan hashes) on every row,
although a walk's rows almost always share one bundle; its cache lived for one row only. The commit phase then decodes the row's segment, and
the command history decodes the same row bytes again with the same row-validating decoder to resolve its commands.

In a write-only TicketDesk profile on the second development machine (8 vCPU, AMD EPYC 7B12), frame-pointer samples of the post-start scrub
put 17% of the scrub thread in the per-row bundle validation and 19% in the command history's second decode. With the reuse below, the scrub
takes 5.8 s instead of 8.1 s (median of six alternating runs, the harness waiting for the scrub before load); in a separate series, bundle
reuse alone took 7.3 s against 8.3 s.

## Decision

1. A complete structural walk may remember the last contract bundle it validated together with the exact stored bundle record (lineage,
   contract version, bundle hash and canonical bytes) it validated, and reuse it for a later row whose stored record is equal. Any other
   record is validated from itself and replaces the remembered bundle. The stored record is still read for every row that needs it.
2. The walk's command history may remember the last segment decoded with the row-validating decoder together with the exact `COMMITS` row
   bytes, shared with the commit phase, under ADR-0283 Decisions 1 and 2: reuse only for byte-equal rows, and only successful decodes.
3. Only a successful validation or decode is remembered. A refused bundle record or row is validated or decoded again, and refused again,
   on every lookup, and does not replace what is remembered.
4. The bundle memo is held by one walk session and by the bounded sampled checkpoint pass; the segment memo by the walk's command history.
   Each holds one entry and is dropped with its holder. Received-segment validation, ordinary reads, writers and recovery replay are
   unchanged.

Validation of a stored bundle is a function of that record alone, and an equal record has equal canonical bytes and identity, so reuse
returns what validating it again would. Segment reuse rests on ADR-0283's argument. Every bundle record and row is still read, and verified
at least once per walk.

## Options considered

1. **Keep validating per row:** rejected; the repeated work is a third of the scrub without checking anything more.
2. **Cache bundles by lineage and version:** rejected; the key does not identify the bytes, so a changed record could be served from the
   cache. Keying by the whole record costs one comparison of its bytes.
3. **Hold every bundle a walk sees:** rejected; memory proportional to catalog history for no gain where rows of one bundle are contiguous.
4. **Share decoded segments across the walk's separate passes:** deferred; the entity-chain and historical passes visit history apart from
   the commit phase, so one remembered segment cannot serve them.

## Consequences

- One bundle validation per run of rows sharing a bundle, and one row-validating decode per segment in the commit phase.
- A walk retains at most one validated bundle and its stored record, and one segment and its row bytes, beyond what it retained before.
- Deferred: the entity-chain and historical passes still decode each segment once each, and the work ADR-0283 deferred remains.

## Standing design tests

- **Interface safety:** nothing on a public surface changes; no caller can select reuse, and the scrub's health states, rate bound and
  cancellation are untouched.
- **Scale:** constant memory per walk, one bundle and one segment; validations fall from one per row to one per bundle change.

## Checks

- `bundle_reuse_follows_the_stored_record_and_never_remembers_a_refusal`: sixteen lookups of one stored bundle validate it once; a record
  with the same identity and changed bytes, and one with the same bytes and another version, are refused on each of three lookups and do not
  replace the remembered bundle.
- `commit_phase_and_command_history_decode_a_shared_segment_once`: over a 16-command segment, the commit phase and the history's lookups
  decode the row once with the memo and twice without, return identical commands, and a refused row is decoded and refused each time.
- The unchanged ADR-0182, ADR-0203 and ADR-0283 walk and scrub proofs, including
  `background_scrub_health_transitions_are_closed_and_cancellable` and
  `segment_reuse_follows_the_row_bytes_and_never_remembers_a_refusal`.
