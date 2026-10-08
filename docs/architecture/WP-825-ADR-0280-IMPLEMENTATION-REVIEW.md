# WP-825: bounded follower acknowledgement validation review

ADR-0280 is proposed and has not been accepted. No acknowledgement runtime
change is implemented. WP-825 remains open and the 24-hour run is unstarted.
The maintainer's standing approval covers necessary native fixes; AGENTS.md
still requires human review before changing read validation and exact acceptance
of a guarantee-tier decision record. This note describes that concrete decision.

## Native failure and diagnosis

Frozen revision `c59c42a0` fails required-host full validation. Ten of thirteen
acceptance steps pass; handbook generation lacks Graphviz, the performance-freeze
checker lacks Python YAML, and workspace tests fail two replication campaigns.
Both missing host prerequisites are installed. A new validation pass is still
required; the failed receipt is preserved.

The bootstrap/crash campaign passes alone with its original bounds. The exact
follower-frontier campaign fails alone at 380 of 408 commits, so concurrency
between test campaigns cannot explain that failure. A separate diagnostic
checkout adds only bounded numeric phase timings and keeps the original test,
workload and 60-second progress watchdog. It fails at the same frontier.
The measured test duration is 102.38 seconds. Its 24 completed source
acknowledgements consume 60.356 seconds; individual calls range from 224 to
3,483 milliseconds. Timing instrumentation is diagnostic evidence only and
is absent from the frozen runtime and proposal.

The source acknowledgement uses the general drained `Barrier::acquire` path.
It walks retained history before journal drain, walks it again after drain,
walks again when starting the write transaction, and validates it again after
staging the hold and source receipt. These population walks hold the native
writer lease. The measured acknowledgement cost and that code path explain
substantial replication delay; they do not establish release throughput or
explain every other historical intermittent CI failure.

The receiver also repeatedly validates a command's stored contract bundle.
That is separate measured work. This proposal targets the population-dependent
source acknowledgement protocol and grants no unrelated catalog cache change.

## Proposed implementation boundary

Extend the existing complete source-validation machinery to a closed advancing
follower-acknowledgement operation. Prepare a complete initial proof and later
checked source deltas outside the writer. Only submit its typed mutation once
prepared, rechecking complete current authority and bounded catch-up in the
existing ordered coordinator turn. The exact old/new hold, current generation,
canonical receipt and successor roots remain atomic and fail closed.

Reuse the existing graph-owned source-control proof lifecycle and capacity:
one certificate and candidate, two pins, compiled bookkeeping, page, frame,
allocation and hold limits. No new per-follower history or snapshot queue.
A checked same-generation acknowledgement may continue its proof; unrelated
or unclassified writes revoke it. Cold validation, cancellation and uncertainty
retain their existing safety requirements. All actual maintenance work remains
accounted and paced.

Registration, retirement, bootstrap attachment, archive acknowledgement,
promotion, startup and offline validators retain their current protocols.
Public surfaces, durable formats, authorizations, queue order, idempotency,
outcome/provenance semantics, report cadence and test deadlines are preserved.

Implementation will coordinate existing allowed storage, storage-API and server
paths with `crates/riffdb-commit/src/**` for typed preparation/admission and
cancellation ownership. The proposal's ADR path is added under the maintainer's
standing necessary-native-fix authorization. The commit path will be recorded
in WP-825's allowed paths upon exact acceptance, before implementation there.
No compatibility fixture diff is proposed.

## Required proof and remaining work

The unchanged replication campaigns must pass on the required host with their
original assertions and deadlines. New package tests must compare continuing
proofs with the independent full validator, count actual old/new rows over
increasing populations, explore queued-writer/cancellation schedules, reject
changed holds and unknown publications, and kill real processes at durable
hold/receipt publication edges. Include requirement tags for the guarantees.
Tests and acceptance run on the required host only.

This proposal does not close any previously unexplained failure. Preserve the
failed CI log and diagnostic hashes; obtain a clean frozen final-source full CI
pass after repair. Then run the unchanged 1,500-second preparation, populated
public backup/restore and full independent reconciliation. The complete short
campaign still needs all seven faults, actual physical retirement, resource
accounting and six final quiet hours before 24-hour readiness.

PR note: Package WP-825 (open); Tier guarantee proposal; Behavior added or
changed: none at runtime; document and propose a repair for native acknowledgement
validation under the writer barrier. Checks run: required-host failed full CI,
unchanged isolated replication campaigns and separate phase diagnostics;
proposal acceptance is pending. Compatibility: public/durable formats and
fixtures unchanged. Hazards and follow-ups: exact ADR-0280 acceptance precedes
implementation; original deadlines, final-source CI and complete rehearsal
remain required. Handbook pages: this review and the lifecycle native-observation
page, indexed from SUMMARY.md.
