---
adr: "0227"
title: Columnar Candidate Publication At A Validated Frontier
status: proposed
tier: guarantee
date: 2026-09-15
accepted: null
acceptance: null
requires: [ADR-0190, ADR-0192]
amends:
  - ADR-0190 decision 9 exact-head publication for schema-bound columnar candidates
  - ADR-0192 decisions 14 and 15 exact-head publication and replacement on head advance
supersedes: []
requirements: [PRJ-005, PRJ-006, PRJ-007, PRJ-008, PRJ-009, PRJ-010, OQ-019, OQ-020, OQ-021]
packages: []
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers: []
---
# ADR-0227: Columnar Candidate Publication At A Validated Frontier

## Context

A prepared immutable V2 candidate at frontier H is discarded if an application
commit advances the authoritative head during construction. With sustained
writes, repeated full rebuilds can prevent publication indefinitely. This is a
high-severity availability issue in an existing capability, within D-001's repair
exception. ADR-0192 decision 14 expressly requires replacement, so changing the
worker alone would violate an accepted decision.

This proposal changes publication eligibility, not the meaning of a frontier or
query freshness. It is unaccepted; existing runtime behavior remains in force
until a human accepts the exact text and the implementation proofs pass.

## Decision

1. A schema-bound columnar candidate may publish at its completely validated
   applied frontier H when H is no greater than the transaction-current
   authoritative head. Publication still compares the complete expected control,
   source, specification, history incarnation, candidate identity, and prepared
   artifact witness. A head change alone does not invalidate that witness.
2. When a same-specification Published generation exists, the candidate must
   advance its applied frontier strictly. Initial and unservable-rebuild
   publication may establish the first valid frontier for that specification.
   No publication may regress the retained source frontier or cross an
   incarnation/specification boundary by inference.
3. The published view reports H exactly. It never claims to contain later
   commits. Every existing query freshness, epoch intersection, causal token,
   policy admission, and continuation check remains authoritative. A query
   requiring a frontier above H waits or returns its existing typed unavailable
   result; this decision grants no stale-read fallback or application bypass.
4. The immutable ROOT-V1 and its full sync/reopen/validation procedure remain
   unchanged. New authoritative commits cannot alter that artifact. The next
   bounded build captures a newer snapshot and contiguous tail using existing
   construction limits. This decision does not introduce an in-place V2 tail,
   another durable root format, or an unbounded catch-up buffer.
5. Retention continues to use the actual selected H, retaining the necessary
   tail. Artifact reclamation continues to respect durable pointers and captured
   views. Publication may not advertise the newer authoritative head as a
   retention frontier.
6. Success, control mismatch, storage failure, and unknown commit resolve the
   exact durable selection before installing a view or reopening the source
   gate. Invalid selected artifacts remain closed/degraded. No predecessor
   fallback, partial partition publication, or guessed success is allowed.
7. Implementation must amend the corresponding SPEC publication language and
   existing exact-head tests together, after acceptance. Generic aggregate
   projection control, follower-local views, encodings, and public surfaces are
   outside this amendment. Work-package assignment remains a human scheduling
   decision; this proposal does not close or expand an existing package.

## Options and consequences

Keeping exact-head equality preserves the current rule but offers no progress
under continuously advancing writes. Pausing all authoritative writers during
artifact construction would introduce an unrelated availability penalty.
Publishing the exact validated frontier permits progress without representing
unapplied commits as applied. Freshness-sensitive queries can still be unavailable
when construction throughput cannot satisfy their requested freshness.

## Standing design tests

- **Interface safety:** applications cannot choose an unchecked root, weaken
  freshness, supply a frontier, or bypass policy. Existing typed freshness remains
  mandatory.
- **Scale:** one existing bounded candidate and published generation; no new
  unbounded replay buffer. Publication work does not grow with the head gap.
- **Recovery:** the complete expected-control CAS and immutable artifact witness
  remain the sole authority for selection and recovery.

## Checks

- Deterministic schedule: advance the head after every completed candidate,
  publish each strictly advancing H, and prove progress without pausing writers.
- Reject H above head, frontier regression, stale expected control, wrong source,
  specification or incarnation, incomplete artifact, and substituted witness.
- Require queries above H to wait/refuse while eligible queries report H; prove
  current row-policy checks and continuation validation remain intact.
- Process crashes before/after artifact sync, control CAS, and view installation;
  reopen exactly the selected artifact or remain closed/degraded.
- Retention cannot prune needed tail; captured views prevent premature deletion.
- Regenerate decision references and update the columnar operations handbook when
  the accepted implementation lands.
