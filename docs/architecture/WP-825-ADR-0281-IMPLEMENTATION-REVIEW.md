# WP-825: accepted bootstrap source custody repair

ADR-0281 exact text and its WP-825 scope extension were accepted by the
maintainer in session on 2026-10-07 ("Accepted"). Native implementation is
authorized after recording that acceptance and scope.
The previously accepted ADR-0280 repair is being validated separately. WP-825
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
promotion validators unchanged. The proposal extends ADR-0280 only for source
read custody. It grants no application interface, wire or durable format change;
no compatibility fixture diff is proposed.

Current WP-825 paths already cover storage API, redb, server, tests and docs.
The accepted scope extension adds only
`adr/0281-bounded-native-bootstrap-source-custody-validation.md` for recording
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
