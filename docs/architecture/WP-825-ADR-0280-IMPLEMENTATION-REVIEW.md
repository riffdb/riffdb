# WP-825: bounded follower acknowledgement validation review

ADR-0280's exact text was accepted by the maintainer in session on 2026-10-07.
Acceptance is recorded separately from implementation; WP-825's approved
commit-layer paths are declared in a following scope-only commit. The bounded native
implementation is under development validation. WP-825 remains open and
the 24-hour run is unstarted. All testing stays on the required host.

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
That is separate measured work. The accepted decision targets the population-dependent
source acknowledgement protocol and grants no unrelated catalog cache change.

## Accepted implementation boundary

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
cancellation ownership. Both the ADR and commit paths were recorded under the accepted scope before
implementation. Native changes use these existing composition boundaries.
No compatibility fixture diff is proposed.

## Numeric native work observations

The internal `riffdb-follower-ack-work-v1` observation contains only bounded
numeric fields. Phase 1 is off-gate preparation; phase 2 is coordinator admission.
Result 0 means ready or completed, 1 means yielded, and 2 means refused.
`off_gate_us`, `writer_wait_us` and `gate_held_us` distinguish off-gate work,
actual native FIFO waiting and writer residence. Off-gate elapsed work excludes
both wait and gate residence. These fields assert no CPU usage or latency
guarantee. Initial and continued row counts come
from the native validation walk. The maximum unsigned value means unknown and
cannot qualify a measurement. This stream carries no hold IDs, source values,
paths or endpoints and grants no mutation authority.

Preparation, acknowledgement and the existing graph's source reclamation share
one proof owner and one scheduling rest deadline. Charge actual native work
using the existing factor of 19 and maximum rest of 19 seconds. Waiting neither
extends the request's original deadline nor reserves coordinator priority.
A completed prepared candidate owns the single proof slot until its real
admission, yield or cancellation. Background source steps defer while that
candidate is ready, preserving the charged rest and avoiding replacement of
its proof. Physical and logical observation lanes continue. An admission yield
releases custody before more preparation; this grants no writer priority.
Restart inherits no proof or scheduling authority. Application interfaces,
wire messages and durable encodings are unchanged.

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

PR note: Package WP-825 (open); Tier guarantee; Behavior added or changed:
accepted bounded source acknowledgement preparation and ordered admission are
under implementation. Checks run: required-host failed full CI, unchanged
isolated replication campaigns and separate phase diagnostics; proposal-only
acceptance passed 11 checks. Acceptance-commit metadata and the regenerated ADR
index pass on the required host. The first development transition tests exposed
an overlapping write-table ownership error; it is corrected and its failed logs
are preserved. The development snapshot after the production bridge repair passes 12 native
acknowledgement tests, including five real process preparation/publication
edges; one explicit-clock pacing test; three coordinator tests covering FIFO,
lost-receipt uncertainty and fenced/stopped refusal; the real-storage production
bridge regression; and four-crate all-target, all-feature Clippy. The unchanged
26-test replication suite passes 23 and fails three. Byte-faithful frontier
replay, exact-provider reads, repeated-kill resume and source revocation pass.
Bootstrap held attachment times out, and two exact/causal cases fail shutdown.
The complete redb library run from the preceding snapshot passes 988 tests,
with one existing synthetic shutdown scale-evidence test ignored. These scoped
development checks do not qualify final-source full CI or any rehearsal. Compatibility: application/durable formats and
fixtures unchanged. Hazards and follow-ups: complete broader validation of the native transition and shared owner,
original deadlines, final-source CI and full
rehearsal. Handbook pages: `docs/configuration.md` explains acknowledgement preparation
and its numeric diagnostics; this review and the lifecycle native-observation
page remain indexed from SUMMARY.md.

The failed unchanged suite and isolated diagnostics exposed a prepared-candidate
handoff defect: background source work could repeatedly renew the shared rest
and replace the completed proof before ordered admission. A deterministic
regression reproduces the defect on the required host before repair. The
source owner now defers background source work only while a completed candidate
holds custody. Broader development validation is still required.

The next broader run passes 22 replication cases and fails four, with frontier
progress still stopped at 12 of 408. A separate private process diagnostic shows
completed preparation but no native admission. The production storage bridge
had inherited the new storage trait's default refusal instead of forwarding the
closed acknowledgement. The bridge now forwards it, and the trait requires an
explicit implementation in every adapter. A real-storage bridge regression
preserves unprepared yielding without inventing an acknowledgement. The real-storage regression and unchanged replication results above validate
this forwarding correction; previous failed runs remain failed.


Shutdown retires source admission before draining replication jobs. A new
regression fails on the required host because acknowledgement custody treats
that irreversible state as a temporary yield. Native preparation, pacing waits
and admission now observe retirement as a terminal typed unavailable refusal,
including rechecking after a raced admission yield. Granted atomic turns retain
the existing completion protocol. No gate is reopened, queue is bypassed,
deadline is extended or shutdown stage reordered. The regression asserts exact
durable hold/epoch preservation and release of nondurable capacity; the existing
explicit gate schedules observe retirement both before and after a grant.
The required-host retirement regression now passes, along with all 13 native
acknowledgement tests, the explicit source-admission schedules, pacing,
production bridge, three coordinator tests and four-crate all-target Clippy.
The unchanged 26-case process suite passes 25 and fails only the initial
bootstrap held-attachment deadline. Both shutdown cases pass. The failed run
remains failed. Separate private bootstrap phase diagnostics are in progress;
initial attachment remains a separate unresolved failure.


Broader scoped acceptance passes all 13 non-test checks, including the complete
handbook check with the repaired host tool environment. Its test stage finds
three deterministic retained-extent regressions: production receipt retirement,
interleaved durable writer progress, and command-audit materialization progress.
Their original explicit-step tests are unchanged. After preserving all three
failures, the failed test stage is interrupted; the receipt remains failed and
cannot qualify the full test scope or final-source CI.

The source rest policy had been activated for storage primitives without a
runtime scheduler. Graph composition now binds the shared scheduler before any
worker starts, including a held-retention configuration. The closed native
acknowledgement owner also binds it before preparation. Binding is monotonic and
cannot erase charged rest or clock-overflow refusal. Actual graph work preserves
the same shared rest, custody and mutation bounds; explicit storage primitives
retain their original stepping semantics. Tests assert mandatory graph binding
and preservation of charged rest across graph/acknowledgement composition.
The required-host repair passes all 14 native acknowledgement tests, 21 source
admission tests, the three unchanged retained-extent regressions, the production
graph binding check, shared pacing, production bridge, three coordinator tests
and four-crate all-target Clippy. The original process suite again passes 25 and
fails initial bootstrap held attachment. A new complete scoped acceptance run
is required; the interrupted preceding test stage remains failed.
The original retained-extent assertions, operation counts and checkpoints are
unchanged. The separate bootstrap read-custody proposal is pending exact review.

The corrected acknowledgement-only snapshot subsequently passes complete scoped
acceptance on the required host: all fourteen checks and 2,855 tests pass under
the stock CI profile. Preserve the preceding failed and interrupted receipts.
ADR-0281 is now accepted separately; its later native implementation still needs
whole-suite and final-source qualification. This scoped pass does not establish
rehearsal readiness.
