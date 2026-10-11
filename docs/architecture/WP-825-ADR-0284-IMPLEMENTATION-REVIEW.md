# WP-825: accepted bootstrap source custody repair

The accepted bootstrap decision is currently numbered ADR-0288; this review
retains its original filename and historical identifier.

ADR-0284 exact text and its WP-825 scope extension were accepted by the
maintainer in session on 2026-10-07 ("Accepted"). Native implementation is
authorized after recording that acceptance and scope.
The previously accepted ADR-0286 repair is being validated separately. WP-825
stays open, no merge or push is requested, and the 24-hour run remains unstarted.

## Measured native limitation

The unchanged required-host replication suite after terminal retirement refusal
passes 25 of 26 cases. Initial bootstrap held attachment exceeds its original
60-second deadline. Both earlier shutdown cases pass. A separate diagnostic
build preserves the original whole process suite, workload and deadlines and
again passes 25 with that same failure. Diagnostic changes only capture private
child stderr and add numeric bootstrap phase/work observations; they are absent
from the implementation source and cannot qualify release performance.

The held source performs 60 complete retained-history validations, totaling
23.525 seconds, each observing source sequence 299. Initial source construction
and hold registration finish at 3.980 seconds; receiver transfer finishes at
27.230 seconds and candidate construction at 44.355 seconds. The following
complete startup validation does not finish before the watchdog. Source walks
repeat for each manifest/page custody check even while source evidence is
unchanged. This proves repeated population-dependent source-read work. It does
not establish that those walks alone cause the deadline failure; receiver
validators and original process assertions still have to pass unchanged.

## Concrete implementation boundary

Add a closed native bootstrap source-read preparation using the existing
shared source-control owner and complete retained-history walk. Prepare a whole
initial proof in bounded off-gate pages. Subsequent reads reuse only actual
qualified source evidence or checked native successor publications. Before each
manifest/page release, recheck the current canonical bootstrap hold, source
lineage, generation, registration/lifecycle authority and artifact identity.
Retirement, replacement, missing evidence and unknown qualification refuse.

Keep one shared certificate and candidate, two pins and the existing compiled
bookkeeping and decode limits. Bootstrap preparation cannot displace a ready
acknowledgement candidate. The closed read operation rechecks at actual native
execution; neither a caller boolean nor an old preparation releases a page.
Existing direct reads retain full validation unless safely composed through
that protocol. Service authorization, page integrity and original deadlines
remain intact. Source reads gain no writer, checkpoint/drain operation,
reclamation license, mutable turn or extra maintenance budget.

Keep bootstrap hold registration and attachment, receiver construction/rebuild,
startup/offline/engine integrity, candidate publication/recovery, archive and
promotion validators unchanged. The proposal extends ADR-0286 only for source
read custody. It grants no application interface, wire or durable format change;
no compatibility fixture diff is proposed.

Current WP-825 paths already cover storage API, redb, server, tests and docs.
The accepted scope extension adds only
`adr/0288-bounded-native-bootstrap-source-custody-validation.md` for recording
this decision. Record it and the accepted dependency in WP-825 before native
implementation. Acceptance is a standalone guarantee metadata change.

## Proof and readiness

First establish a failing regression that counts real source-validation rows
across multiple pages. Compare every reused/continued proof against the full
native validator. Prove stale/corrupt/retired hold refusal, no release from an
incomplete walk, fixed candidate/pin capacity, cancellation and coexistence
with a prepared acknowledgement. Keep complete real bootstrap crash proofs.

Then pass the original process suite, scoped acceptance and frozen final-source
full CI on the required host. Preserve previous failed receipts as failures.
Neither an isolated pass nor a modified watchdog closes the failed whole suite.
The unchanged preparation, populated public restore, seven continuous-load
faults, actual retirement, complete independent resource model and six final
quiet hours in the short rehearsal remain required before 24-hour readiness.

PR note: Package WP-825 (open); Tier guarantee; Behavior: accepted bounded
native read-only bootstrap custody, shared acknowledgement coexistence and
atomic receipt-ancestry cleanup ownership; Checks: native oracle/refusal/race
regressions, server bootstrap tests, coordinator checks, four-crate Clippy and
eleven metadata gates pass on the required host. The deterministic cleanup
regression fails before repair; the unchanged whole process suite subsequently
passes with the original stack reservation. Final-source full CI remains pending.
Compatibility: application/wire/durable interfaces and fixtures unchanged;
Handbook: configuration, this review and the lifecycle observation page are
indexed from SUMMARY.md; Hazards: failed process receipts remain failed,
exclusive abort diagnosis, final-source full CI and complete rehearsal remain
outstanding.

## Accepted implementation in progress

Acceptance and the exact decision path are recorded separately from runtime
changes. The native read custodian now uses the shared source owner, one
bounded validation page per advance and a fresh custody check after page I/O.
Its nondurable ownership token cannot validate source evidence. A ready
acknowledgement candidate makes the reader yield; cancellation preserves that
candidate and releases only the cancelling reader's incomplete pin. An
acknowledgement writer turn yields if the proof mutex is occupied rather than
waiting for foreground validation while owning the coordinator turn. Read-only
proof work is observed separately and leaves charged maintenance rest intact.

The three named tests use real held artifacts and independent complete native
validation, cover increasing source populations, page transcript integrity,
checked successors, unknown publications, changed/missing holds, attachment,
corruption, fencing, retirement, candidate coexistence and cancellation. They
pass on the required host, alongside the additional two-reader test with an
attachment raced after actual page I/O. All six custody-filter cases, fourteen
acknowledgement cases, seventy-one server bootstrap cases and three coordinator
cases pass. Four-crate all-target, all-feature Clippy passes after boxing the
fixed-size internal manifest variant; public manifest values are unchanged.
All eleven metadata checks pass, including acceptance-commit separation.

The prior acknowledgement-only scoped acceptance completes successfully:
fourteen checks and 2,855 tests pass under the stock CI test profile. That receipt
does not qualify the subsequently added custody implementation. Its original
whole twenty-six-case replication suite passes twenty-five, including the
previously failing bootstrap held attachment, and fails the concurrent promotion
fencing case with transport unavailability. A separate diagnostic run preserves
the workload and deadlines and captures an actual `riffdb-columnar` stack
overflow and daemon abort during fencing. That diagnostic run also misses two
existing replication deadlines; all three failures remain unresolved evidence.
Native stack investigation is in progress. No deadline or assertion is relaxed.
The exact accepted body and all original deadlines and workloads are unchanged.

## Native receipt cleanup race

A deterministic schedule over real receipt-source nodes finds an independent
cleanup defect. Failed `Arc::try_unwrap` retains a reference. If the other owner
releases before that failed result is dropped, dropping the result can recursively
destroy the remaining ancestry. Thirty-two controlled handoffs produce thirty-three
nested cleanup calls despite the intended iterative loop. The regression also
checks that every node and shared source payload is released.

The handoff now uses `Arc::into_inner`, atomically releasing or transferring the
reference before the next cleanup step. Production stack reservations remain
384 KiB. The test hook and depth witness compile only in the native unit-test
build, and the schedule uses explicit channel rendezvous without sleeps. The
new regression and all eleven source-cursor cases pass on the required host.
Sixty-four columnar tests, four-crate Clippy and the unchanged whole twenty-six-case
replication process suite also pass with the original stack reservation. All
thirteen non-test acceptance gates pass, including handbook, package scope and
obligations. The old cursor architecture guard fails because it requires the
obsolete ownership API; its exact consequence-test path is recorded under the
standing necessary-native-fix approval, and its assertion is updated to require
the atomic handoff. All fifty-nine storage architecture tests then pass. The
scope, acceptance-commit separation, coverage, obligations, file-size and
performance gates pass against the separately recorded consequence-test scope.
Final-source qualification remains outstanding.
An earlier fixed 2-MiB columnar-stack trial passes its native checks but is
excluded from the final implementation. This cleanup race is proven separately;
it is not an exclusive diagnosis of the earlier process abort. Final-source
full CI and complete rehearsal still have to pass.

## Frozen checkpoint result

Native source `0d26ff43` subsequently passes all twelve full-acceptance gates on
the required host, including complete CI, with the source clean. The shipping
release build passes. The unchanged populated preparation then fails
`source_bytes_sample_gap` before its first complete five-minute window: the
source-byte producer emits one complete in-load observation while logical and
physical observations continue. This is failed native preparation evidence;
it establishes neither normal-window throughput/latency nor populated recovery.
The remaining work is to diagnose the production accounting/progress blocker.
Complete campaign integration remains deferred, WP-825 remains open and no
24-hour run has begun. The earlier abort is still not exclusively diagnosed.

A separate diagnostic repeats that gap failure and records a larger accounting
candidate expiring after ten pages: 2,560 of 10,631 remaining eligible receipts
are checked before its 30-second capture lifetime ends. Completed-certificate
reuse and checked-cut rebasing operate earlier in the run. This isolates a
native accounting progress failure mechanism; the dominant costs and safe
repair remain unproven. Diagnostic binaries qualify no release performance.

## Captured read-only observations during charged proof rest

Source-byte accounting is an observation under ADR-0279 decision 6, not source
mutation-proof preparation. Restore its already captured page continuation
beside logical/physical observations during the existing lane's wait/rest.
Proof preparation and writer work retain shared source pacing under ADR-0286
and ADR-0284. The deterministic negative/positive regression preserves the exact
charged deadline, source-proof progress, writer census and authoritative history.
This is a D-003 implementation choice: accepted decision text, public surfaces,
durable identities, source qualification, canonical validation, limits and
writer ordering remain unchanged. No codec shortcut from the cost diagnostic
is included. Required-host measured validation and final-source CI remain open.

PR note: Package WP-825; Tier guarantee; Behavior: continue already captured
read-only source-byte observations during source-proof cooldown without advancing
mutation authority; Checks: preserved failing-before regression, targeted native
observation/pacing/architecture tests, 1,322 active storage tests, 19
retention-worker tests and all-target storage Clippy on the required host;
Compatibility: existing public interfaces, durable formats,
fixtures and observed limits; Updated handbook: sustainable TicketDesk milestone;
Hazards/follow-ups: corrected decision-note YAML metadata acceptance,
unchanged workload preparation and final-source full CI before campaign
qualification.


## Ready read-only pages and checkpoint polling

The required-host candidate still fails byte observation. Its separate numeric
diagnostic shows a later 77,103-row interval cannot fit 302 mandatory 100-ms
polls inside the unchanged 30-second capture lifetime, even without read work.
Two deterministic scheduling regressions fail before repair. Distinguish ready
captured read-only pages from paced source-proof preparation. Process ready
pages inside existing rest/wait, checking cancellation and actual checkpoint
completion between bounded pages. Keep the full writer-rest deadline, source
preparation pacing, capture cadence, freshness, gap and all workload limits.
This is a D-003 implementation choice under ADR-0279 decision 6 and the already
accepted source pacing: it changes no validation, writer budget, source proof,
durable format, ordering or application interface. The hidden readiness query
confers no authority and permits no guarantee opt-out. The twelve native
source-byte tests, 23 worker tests, storage/server Clippy, 59 architecture tests
and twelve metadata gates pass on the required host. The unchanged preparation
completes two load windows and ten byte samples, including a 78,373-row capture
in 2.217 seconds, then fails a later byte gap. Its failure is preserved.
Populated recovery and final-source full CI remain pending.

PR note: Package WP-825; Tier guarantee; Behavior: avoid a checkpoint poll before
each ready captured read-only page while keeping source-proof waits and full
writer rest; Checks: unchanged required-host failure, bounded diagnostic and
two failing-before deterministic regressions; Compatibility: no durable,
fixture or application interface changes; Updated handbook: sustainable
TicketDesk milestone; Hazards/follow-ups: diagnose the later native accounting stall, repeat the
unchanged preparation and run final-source CI before campaign qualification.


## Fixed-prefix observation and classified checkpoint replacement

The unchanged ready-page runtime reproduces a later observation gap with a
complete exact byte certificate still qualified and no capture expiry. Its
mutation-proof continuation can return before the accounting capture step.
A deterministic native regression fails before repair: a completed fixed prefix
does not refresh while unrelated added receipts require further proof pages.
Permit a new observation on the original cadence during an ordinary paced source
turn only when the completed interval, current qualified immutable pin, existing
finite checkpoint license and current derived/durable eligibility agree exactly.
Reuse no unknown bytes and extend no pending capture lifetime. Source proof,
writer rest, deletion authority and the two validation-pin bound remain intact.

A separate failing-before native regression prepares a finite checked cut, then
installs a real classified checkpoint replacement. Exact checkpoint identity
rejects that otherwise qualified cut. Use the existing license validator with
checkpoint metadata bound to the actual admitted source pin. Recheck the exact
licensed point, original coverage, qualification, current source proof and holds.
Read bounded metadata without another capture, drain or checkpoint under the
writer. Remove the redundant prepared checkpoint copy; the finite license still
owns its original evidence. No cut ceiling is extended or independent progress
manufactured.

These are D-003 implementation choices under ADR-0279 decisions 2 and 6 and
ADR-0268's closed continuation. Accepted text, ordering, durable formats,
application interfaces, source/audit validation and all original bounds remain
unchanged. Initial regression fixtures repeated equal writes or mixed physical
and journaled fixture state; those failed attempts are retained but prove neither
bug. Corrected native fixtures establish both intended failures. Positive scoped
checks, unchanged preparation, populated recovery and final-source CI remain
pending. WP-825 remains open and no 24-hour run starts.

PR note: Package WP-825; Tier guarantee; Behavior: refresh an exactly known fixed
source-byte prefix independently of unrelated tail proof, and admit classified
checkpoint replacements through existing finite-coverage validation; Checks:
unchanged required-host gap reproduction and two corrected failing-before native
regressions; Compatibility: no public interface, dependency, fixture or durable
format changes; Updated handbook: sustainable TicketDesk milestone;
Hazards/follow-ups: positive native/cancellation/hold checks, unchanged measured
preparation, populated recovery, final-source CI and complete short rehearsal.


The first positive broad storage run passes both new regressions but reports
998 passing unit tests, one existing ignored test and one offline-upgrade custody
failure after its fixture owner closes. The same custody test passes in isolation.
An explicit-pipe C3D operating-system check proves that a close-on-exec file lock
remains held after parent close while a child owns the inherited descriptor, and
can be reacquired after that final owner exits. This is consistent with parallel
crash-test process creation retaining fixture custody transiently. Isolate the
complete custody fixture after exec in one child, retaining every original
refusal, receipt absence, marker absence, unchanged-byte and successful-upgrade
assertion. Production upgrade and lock behavior are unchanged. Preserve the
failed suite; repeat the suite at its original concurrency before qualification.


The corrected concurrent storage suite then passes 1,324 active tests with two
unchanged ignored tests, all 23 retention-worker tests, storage/server Clippy,
59 architecture tests and twelve metadata gates. A further failing-before native
regression identifies overly broad retirement: an older qualified mutation proof
revokes a newer completed exact observation. Preserve that completed observation's
actual publication, capture time, bytes and cadence; it retains no reader pin.
Pending candidates keep the original fail-closed sharing rule. Finish the new
fixed-prefix observation's zero-row continuation before mutation preparation can
capture a checkpoint, preserving the total two validation-pin bound.

A distinct failing-before regression reproduces the same capture starvation in
the checkpointed commit profile. Place this read-only refresh before proof
continuation in the shared entry point for both existing profiles. Neither
profile changes source/audit validation, mutation admission or writer pacing.
These remain D-003 choices under the same accepted decisions. Focused positive
checks and final-source measured/CI qualification remain pending; no endurance
readiness is claimed.


The completed-certificate and two-profile repair passes all 15 source-byte tests,
12 source-continuation tests, five checkpoint-license tests, 23 retention-worker
tests, 59 architecture tests and all-target storage/server Clippy on the required
host. This focused pass follows the preserved 1,324-test concurrent storage and
process-level pass on the initial candidate. Final-document metadata, unchanged
shipping preparation, populated recovery and final-source full CI remain open.
Freeze the source and shipping binaries before measured qualification; no
24-hour readiness, merged package or completed rehearsal is claimed.
