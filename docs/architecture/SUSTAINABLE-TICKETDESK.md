# Sustainable TicketDesk milestone

This is the implementation handoff for the milestone selected on October 1,
2026. Its architecture records were **accepted** in session that day. WP-822
adds the optional [durable local archive](../operations/LOCAL-OUTBOX-ARCHIVE.md)
and production dispatch worker; scoped acceptance and full CI pass. Source
receipt reclamation and exact-provider retirement are implemented and validated.
WP-823 and WP-824 are complete; the real workload and rehearsal remain open.
The evidence checker is preparatory acceptance code.

Captured native source-byte pages now continue during source-proof cooldown,
using the same read-only wait/rest continuation as logical and physical
observations. They retain their original qualification, exact byte/row/page,
capture cadence, freshness and observation-gap checks. Source-proof preparation
and writer turns retain the charged source scheduler and full writer-residence
rest. Observation work grants no mutation, checkpoint or reclamation authority.
The worker distinguishes already captured read-only pages from source proof
waiting for its preparation budget. Ready read-only pages advance without a
checkpoint polling delay per page, checking cancellation and completion between
bounded pages; proof-only waits retain their polling and shared pacing. The
original wall-clock writer rest still runs to its full deadline. Required-host
scoped checks pass. The unchanged preparation completes two load windows and
ten in-load source-byte samples, then fails a later source-byte gap. The largest
completed capture checks 78,373 receipts in 2.217 seconds. Investigation of the
later stall, populated recovery and final-source full CI remain open; this
progress does not establish endurance readiness.

The next native candidate permits a fresh observation of an already exact fixed
source-byte prefix while unrelated appended receipts remain under mutation-proof
validation. It requires the original cadence, current qualified immutable pin,
finite checkpoint coverage and unchanged exact eligibility. A changed interval
or lost qualification remains unknown. Classified checkpoint replacement uses
the existing finite-coverage validator rather than exact root equality; every
cut retains current source/audit proof, holds and bounded ordered admission.
The initial two corrected native regressions pass with the concurrent native
suite. Further failing-before regressions cover the checkpointed profile and an
older qualified proof retiring a newer completed observation. Apply refresh to
both profiles, retain the completed observation's actual identity and deadline,
and finish zero-row reuse before another checkpoint capture. Pending candidates
retain their original fail-closed sharing rule and the two-pin bound remains.
Focused native checks pass: 15 source-byte, 12 continuation, five license and
23 worker tests, 59 architecture tests and storage/server Clippy. Final-source
CI, original measured preparation and populated recovery remain pending; this
candidate grants no new
application control, mutation authority or endurance qualification.

The campaign controller timeline freezes the existing seven fault kinds and
their order, admits one active action, requires an observed injection release
and reconciliation, and charges actual elapsed recovery. A late pulse, overlap,
clock rewind, missed action, overrun or action in the final six hours refuses.
Deterministic timeline tests are preparatory; actual public fault handlers,
resource reconciliation and the measured short campaign still need integration.

The actual native two-command prune fixture drives the production logical-byte
emitter before removing eligible history. The preparation gate runs
`scripts/check-ticketdesk-lifecycle-native-bytes`, independently parsing actual
logical, source and physical production records from their native fixtures.
The existing before/after table comparison still proves logical byte meaning
and retained evidence. This is local fixture evidence, with no campaign or
required-host qualification.

## Accepted finite checkpoint licensing

ADR-0279 (`adr/0279-finite-checkpoint-licensing-for-sustainable-source-reclamation.md`)
was accepted in session on October 7, 2026. The frozen native regression on the
previous implementation removes only 256 of 1,030 independently covered
receipts after 128 bounded maintenance attempts. That failure is preserved.
The new online owner retains a finite license over the earlier observed prefix
subsumed by a later actual independently durable checkpoint. Each acknowledged
cut consumes at most 256 receipts and 32 MiB of checked canonical preimages;
current source validation, checkpoint coverage, fencing and all holds still
govern admission. Existing holds pause consumption. Changed or missing evidence
revokes the license, and its own cuts and checkpoint materialization cannot
renew it. Both Standard and Hardened online owners use finite licensing;
offline full validation and authoritative atomic write paths remain intact.

Exact read-only source accounting now continues through native classified
appends and this owner's acknowledged, checked removed preimages. A complete
certificate retains no snapshot between captures. A changed ceiling without
exact bytes, lost qualification or incomplete work requires bounded validation;
missing evidence never becomes zero. The owner keeps one certificate/candidate,
shares validation pins and retains the 64-KiB bookkeeping capacity and two
validation-pin limit. Accounting grants no deletion authority.

The native `riffdb-retention-source-bytes-v2` record keeps canonical receipt and
key bytes separate from logical and physical accounting. It identifies the
actual checkpoint and composite publication with numeric kind and four exact
frame-hash words. `reused_rows` counts qualified rows carried into this capture;
`checked_rows` counts newly scanned receipts; `subtracted_rows` counts scanned
or reused receipts removed through acknowledged preimages during that capture.
The retained interval obeys `rows = reused_rows + checked_rows - subtracted_rows`.
`pages` counts actual new scan pages, so complete reuse can have zero pages.
The V1 reader and its original scan/page meaning remain available. Collectors
freeze one version and basis per observation stream and refuse mixed evidence.
The 30-second capture freshness and 95-second observation-gap limits remain.

The earlier unchanged required-host attempt still fails source-byte coverage:
35,249 receipts removed, final source plan resume 34,994 and eligible ceiling
336,768, and 1,918,873,600 reference-protected allocated bytes in its last
physical observation. Preserve that failed evidence. These native changes do
not establish required-host throughput, a complete resource model, physically
retired generations or readiness for the 24-hour run.

The corrected large-receipt fixture proves that both profiles shrink each
quantum to the actual canonical 32-MiB bound and finish the finite prefix.
The earlier fixture counted raw before-image payloads as if they were retained
in receipts; canonical replacements retain the prior hash. Its below-limit
failure is preserved, and the corrected fixture uses an independently measured
above-limit canonical interval. Actual native V1 and V2 emitters pass the
independent diagnostic readers, and all 184 lifecycle tests pass. Scoped
non-test acceptance passes all 14 checks and all-target Clippy is clean.

The broad Cargo unit run passes 975 tests but fails the new fixture's original
size assertion and an existing archive-publication crash test with native
`Unavailable` during selected replay setup; one existing test is ignored.
The corrected byte fixture and unchanged isolated archive test pass separately.
The archive refusal remains unexplained and is preserved as a native concern,
not retried by the workload or classified as successful CI. The full configured
native test suite and final-source full CI still have to pass.

The configured recovery matrix then catches a native starvation regression on
`b7d42046`: the owner removes no receipts during 24 deterministic interleaved
writer/checkpoint turns, then removes 628 after writers stop. Requiring the
validation snapshot to be the newest checkpoint caused this regression. The
repair shares the accounting candidate's qualified immutable pin with the
completed source proof before reading actual independently durable checkpoint
evidence. It also shares the current admission pin before submission and
releases completed validation pins before successor adoption. This preserves
both progress with continuing writers and the two-validation-pin bound.
The unchanged real-command/provenance/outcome/event regression passes after
repair; a separate oracle proves old-pin release with unchanged capture time,
ceiling, counters and exact bytes. These passes do not qualify the campaign.

The first detached local CI launch produces no full-CI log or completion
receipt. Its launch and dependency evidence are preserved as incomplete.
The frozen `b7d42046` host build has started but is unqualified; no preparation
or workload has launched on those artifacts. Final CI must use an isolated
clean checkout of the repaired source, with a retained runner and actual exit
and hash-bound completion receipt.

PR note: Package WP-825 (open); Tier guarantee under accepted ADR-0279;
Behavior: finite native online consumption and exact V2 read-only accounting
continuation; Checks: preserved failing native prefix regression, independent
pinned byte oracle, exhaustion, hold, lineage, qualification, expiry and overflow
regressions, legacy-reader and mixed-version collector tests; Compatibility:
V5, writer 4, earlier durable fixtures and public application interfaces unchanged;
Updated handbook: this page; Hazards and follow-ups: final-source scoped and full
CI, unchanged required-host preparation and every measured rehearsal gate remain
required. No 24-hour run has started.

## Missing native byte evidence stops preparation promptly

The unchanged 1,500-second required-host diagnostic on native `f153ae48`
completes 150,000 commands and 600,000 reads with zero retries and no
post-duration work, then fails `source_bytes_sample_coverage`. Only one complete
source-byte sample appears during load; logical and physical collectors each
retain 24 samples. The source producer emits no further complete record.
The failed population and private original observations remain intact. This
is failed preparation evidence; state/archive reconciliation and the populated
restore gate were not reached.

The collectors previously checked the 95-second gap only when another native
record arrived or at final reconciliation. A permanently silent producer could
therefore consume the entire diagnostic duration before refusal. Source,
logical and physical log polling now checks that same gap against actual last
observation, including the missing-initial-sample case. It does not substitute
zero, infer bytes, relax freshness or permit qualification. Native producer and
reclamation repairs remain necessary.

The corrected regression fails six silence assertions on the predecessor.
Three parameterized tests now pass for all three collectors: missing initial
record, silence after one actual record, and continued actual fresh records.
All 180 lifecycle tests pass. The first baseline also had an incorrect physical
fixture incarnation; the corrected baseline has six semantic failures and no
fixture errors. Those attempts remain private and distinct.

The latest local frozen full-CI attempt has no exit or completion receipt and
no surviving runner. It is preserved as interrupted/incomplete. The earlier
full attempt failed the operational adapter because its owned socket path was
too long; the adapter passes independently with a short root. Neither result
establishes a final-source full-CI pass. The 24-hour run remains unstarted.

PR note: Package WP-825 (open); Tier internal; Behavior: refuse silent native
byte producers during polling at the existing observation gap; Checks: corrected
predecessor regression fails, three parameterized tests and all 180 lifecycle
tests pass; Compatibility: native formats, observations and limits unchanged;
Updated handbook: this page; Hazards and follow-ups: native reclamation/accounting,
final-source CI and all measured rehearsal gates remain open.

## Restart preparation waits for actual ordinary readiness

The strict final-worker diagnostic initially fails graceful restart with closed
`RDB-STORAGE-0101`: its retry races the native Health-only initializing stage.
The specification deliberately rejects ordinary admission before activation.
The native classifier remains strict. The small interruption controller now
publishes its private resume barrier only after observing actual daemon
readiness, after the worker has emitted one actual failed transport submission.
The worker then resubmits the original immutable input. Waiting for the barrier
counts against the original 60-second recovery deadline; missing or malformed
control cannot create success. This is lifecycle orchestration, with no retry
of an admitted native storage failure and no initialization-authority bypass.

Actual locked debug-artifact fixture diagnostics now pass graceful restart,
process kill and capability rotation. Restart recovery takes 561,484,518 ns and
kill recovery 518,601,038 ns, each with one settled failed submission, exact
original identity and public state reconciliation. The unchanged native server
and CLI come from `f153ae48`; the controller and worker overlay are independently
hash-bound. The failed first attempt remains separate and intact. These small
local fixtures do not prove required-host performance, continuous-load fault
coverage, populated restore or the complete campaign.

Two explicit async barrier/refusal tests and all 32 native workload tests pass.
The general recovery loop also bounds its delay future by the remaining
original deadline, keeping the fixed attempt ceiling and native refusal
classification. Final-source CI and the measured campaign remain required.

PR note: Package WP-825 (open); Tier internal; Behavior: resume interrupted
fixture input only after observed ordinary readiness while preserving the first
real transport failure and the original deadline; Checks: actual predecessor
fixture fails, 32 native workload tests and three actual public fixture
invocations pass; Compatibility: public lifecycle refusal, authorization,
command identity, native errors and durable formats unchanged; Updated handbook:
this page; Hazards and follow-ups: full checks and continuous-load integration,
ADR-0279 native validation, final-source CI and all measured rehearsal gates.

## Interruption recovery preserves native failure visibility

The generated load worker now recovers only actual transport interruption or
an uncertain outcome during a declared restart/restore fault. It keeps the
same immutable input and idempotency key, exact attempt accounting, 240-attempt
ceiling and 60-second deadline. A generic retry action no longer authorizes the
harness to retry storage unavailability, capacity overload, projection failure,
freshness failure or a native deadline. Those failures stop the invocation even
when another operation has already entered recovery. The refusal retains only
the closed registered error code, making the native cause diagnosable without
printing error context.

The predecessor regression accepts `storage_unavailable` during a planned
interruption and fails the required refusal assertion. All 30 native workload
tests pass after restricting the classifier, including positive transport and
typed uncertainty recovery and exact input preservation. Actual public restart
and restore measurements against this final worker remain required.

PR note: Package WP-825 (open); Tier internal; Behavior: fail the rehearsal on
native database failures rather than retrying them under a fault marker; Checks:
predecessor regression fails, 30 workload tests, all-target/all-feature Clippy
and all nine scoped acceptance checks pass; Compatibility: public
error registries, application semantics and uncertainty recovery unchanged;
Updated handbook: this milestone handoff; Hazards and follow-ups: final-source
CI and actual fault qualification remain required.

## Complete bounded load observation streams

The private `CampaignLoadStream` reader accepts exactly one native readiness
record, the declared 73–288 contiguous five-minute window records, a reconciled
terminal record and end of file. Each line remains bounded to 4 KiB; the total
byte ceiling derives from the exact window count. The separate small preparation
and recovery readers keep their existing limits.

The reader checks the native worker's 28 load slots and four auxiliary slots,
exact offered counts, actual completion and retry arithmetic, terminal totals,
integer widths, duplicate fields and record order. It preserves actual retries
and latency overflow for the campaign checker to judge against measured fault
intervals and unchanged service levels. A stopped record without end of file,
missing window, trailing record or malformed observation cannot complete.

Seven synthetic framing/accounting tests cover a full 288-window stream above
64 KiB and hostile or incomplete evidence. They do not prove actual load,
transport success, faults or campaign qualification. The actual controller must
use this reader, retain the child's exit result and reconcile observations with
independent state and resource evidence before qualification.

PR note: Package WP-825 (open); Tier internal; Behavior: exact day-sized private
load framing without enlarging small diagnostic readers; Checks: missing-module
baseline fails, seven semantic reader tests, all 175 lifecycle tests and all
11 scoped acceptance checks pass; Compatibility: native
schemas, worker limits and campaign service levels unchanged; Updated handbook:
this milestone handoff; Hazards and follow-ups: actual campaign integration and
all final-source qualification gates remain required.

## Lifetime bounds for owned campaign processes

Private transient and persistent launchers can freeze a Linux hard `RLIMIT_AS`
ceiling before replacing the launcher with the actual worker in the same
process. The launcher verifies its complete virtual high water after enforcing
that ceiling and sends a bounded proof before execution. A missing proof,
invalid ceiling or launch refusal fails the invocation. Restarts retain separate
proofs and exit records; exact process kills still retain kernel RSS high water.

The exit record distinguishes three quantities: the unknown measured
`exit_allocator_peak_bytes`, the enforced worker bound
`worker_exit_allocator_peak_upper_bound_bytes`, and the full child-lifetime
`exit_allocator_peak_upper_bound_bytes`. The last remains unknown when the
parent was unconstrained at fork. A finite inherited hard limit can bound birth;
the proof also includes the parent's existing mapped image, since Linux permits
lowering a limit below existing mappings. The worker's smaller limit takes effect
in the launcher. These are conservative address-space bounds including mappings,
stacks and reservations, never allocated live bytes.

The final campaign must freeze actual limits and account independently for every
incarnation and descendant. A limit refusal remains a workload failure, rather
than a reason to retry at a larger limit. Existing invocations without a frozen
ceiling retain unknown exit bounds.

The first missing-capability test fails on the predecessor. Nineteen child
usage tests, all 177 lifecycle tests and all 11 scoped acceptance checks pass,
including allocation refusal, refusal to raise the hard limit, exact kill, missing-proof cleanup and independent
restart records. A semantic regression also proves that an unconstrained
parent cannot confer a full-lifetime bound. The corrected proof covers the
finite inherited-limit case. A lowered-parent diagnostic refuses under memory
pressure and is not passing accounting evidence. These checks establish no host
memory ceiling or campaign qualification. Records retain `qualifies_complete_resource_model: false`.

PR note: Package WP-825 (open); Tier internal; Behavior: optional private
kernel-enforced lifetime upper bounds for owned process evidence; Checks: child
usage and lifecycle tests above; Compatibility: public database interfaces,
fixtures, service levels and unlimited diagnostic calls unchanged; Updated
handbook: this milestone handoff; Hazards and follow-ups: integrate frozen
limits, all owner incarnations and independent accounting into the actual
campaign, then repeat final-source checks and measured qualification.

## Journaled bounded source reclamation

The preparation on `e6a4005b` failed `source_bytes_sample_gap`: no fresh
source-byte observation appeared under load, while logical observations remained
fresh. Its scoped checks and full CI passed; that evidence does not qualify the
failed preparation. Its populated restore was cancelled before launch.

The maintainer accepted ADR-0278 exactly in session on October 7, 2026.
The Standard profile now validates one immutable acknowledged composite source
and publishes a closed source cut through the existing ordered journal. The
Hardened and offline owners retain their drained native barrier. This removes
the online requirement to find an active suffix small enough to materialize
before source progress. The deterministic regression uses an actual independent
checkpoint followed by 257 acknowledged audit transitions; its cut preserves
the checkpoint identity and every later acknowledged receipt.

Each cut removes at most 256 original receipts and at most 32 MiB of resolved
canonical preimages. A bounded proof records their exact lengths and digests,
predecessor history and retained resume point. Full source and control-evidence
validation, every current hold, the fixed FIFO admission slot, writer rest,
independent durable checkpoint licensing and physical reference protection
remain mandatory. A cut's own acknowledgement or checkpoint materialization
cannot license another cut. Replaced validation pins retire; weak source indices
retain no additional physical ancestry after its last owner releases it.

Logical journal frame V5 adds only the closed SourceReclamation kind. Writer 4
requires the exact backup-bound writer-3 offline upgrade described in
[Compatibility](../compatibility.md). Application and administration frontiers,
commands, outcomes, provenance and public export encodings retain their
contracts. Recovery validates durable bytes and actual removed preimages;
process-local certificates grant no replay authority. The V3 recovery walk
streams frames in two exact passes rather than retaining the entire suffix or
walking unrelated source populations. Complete integrity scrub and first-access
validation retain their separate responsibilities.

Native frame admission includes the original decoded source, indexed receipt
fold, canonical scratch and one nested terminal/preimage reconstruction window
inside the existing compiled 64-MiB recovery ceiling. The live buffered writer
applies the same check before publication. This conservative ownership bound can
refuse large work even below the 16-MiB wire ceiling; it cannot acknowledge work
that its checked recovery window would refuse. An over-budget legacy suffix
also refuses before replay or reanchoring. These are native typed limit defects,
with no harness exemption or configurable recovery-memory override.

Implementation checks are ongoing. Targeted canonical, original-preimage,
source-control, crash and maximum-receipt checks have passed; final-source scoped
acceptance and full CI, human fixture review, unchanged required-host preparation,
populated restore and the complete short campaign remain required. WP-825 stays
open. No 24-hour run has started.

PR note: Package WP-825 (open); Tier guarantee; Behavior: bounded journaled
online source cuts and streaming recovery under ADR-0278; Checks: targeted
native validation, exact frozen codec vectors, actual maximum receipt and full
suffix-index retirement tests; Compatibility: current writer 4, readable frozen
journal V2/V3/V4 and new V5, exact immutable-backup predecessor upgrades;
Updated handbook: this page and Compatibility; Hazards and follow-ups: remaining
recovery accounting, final-source CI, fixture review and actual qualification
remain unproved.

## Continue installed source checkpoints before admission

The unchanged preparation on `cde795b5` fails `source_bytes_sample_gap`.
Logical observations remain fresh under load, but source observations stop
after one sample at approximately nine seconds. The native drain reports
repeated oversized suffixes, and its shutdown census also reports bounded
catch-up refusals. Preserve the failed population and artifacts; the queued
populated restore is cancelled without launching. No qualification follows.

A deterministic regression establishes a separate continuation gap: a complete
source certificate does not start validating an already installed successor
checkpoint until drained admission succeeds. A newer oversized active suffix
can block that admission even though the installed checkpoint is available
for read-only validation. This is a native scheduling defect, not a reason to
relax the harness freshness gate or drain bounds.

At the beginning of a native attempt, an existing complete certificate now
continues on a newer stable installed checkpoint before requesting the writer.
It uses the existing actual-mutation qualification, checked successor metadata,
canonical validators and bounded pages. An incomplete candidate finishes its
original pin before replacement; changed qualification retires the candidate.
This follows ADR-0268's qualified append continuation and D-003. No authority
is gained until the existing exact-current drained barrier and complete fencing
checks pass. The off-gate wait continuation still captures no new root.

PR note: Package WP-825 (open); Tier guarantee implementation; Behavior: advance
qualified installed-checkpoint catch-up before source admission; Checks: failing
held-writer regression, exactly 300 new receipts and 300 audit rows, pin release,
unchanged oversized-suffix refusal and all seventeen source-drain tests pass;
six continuation tests, twenty-six source-control tests, all-target storage
Clippy and all twelve scoped non-test checks pass; Compatibility: public
interfaces, durable formats, writer ordering, drain and deletion bounds, rest
and freshness unchanged; Updated handbook: this milestone page; Hazards and
follow-ups: this local repair does not prove required-host progress. Repeat
final-source CI and unchanged preparation, then populated restore and the
remaining complete campaign gates. No 24-hour run has started.

## Captured byte progress during native maintenance waits

The debugging preparation on `8119dfcc` fails `logical_bytes_sample_gap` and
also produces no fresh source-byte sample during load. Its 1,269 native storage
tests and all thirteen scoped checks pass; this does not qualify the host.
The unchanged failure and private artifacts are retained.

Two deterministic regressions establish omitted progress: an incomplete
logical-byte probe remains after receipt validation and the physical census
have no work, and a completed-publication wake returns before advancing its
captured observation page. The native worker previously used only receipt
validation and physical-census flags to schedule those off-gate pages.

The graph-owned lane now reports pending captured receipt validation,
logical-byte, source-byte and physical-census work independently of mutation
authority. During existing writer rest and checkpoint waits it advances one
existing bounded page per component, releases completed or expired pins, and
checks again. It captures no new root or census there. A publication wake
checks metadata and cancellation before page work and still permits one
already captured page before returning.

Full writer residence rest, checkpoint deadlines, 256-row pages, byte ceilings,
30-second freshness, capture cadence and every qualification gate are unchanged.
`receipt_validation_pending` retains its exact validation meaning; byte probes
confer no validation, checkpoint or deletion authority. This is an implementation
choice within ADR-0268, ADR-0271, ADR-0272 and D-003.

PR note: Package WP-825 (open); Tier guarantee implementation; Behavior: schedule
captured byte pages through native off-gate waits; Checks: failing logical-pending
and publication-wake baselines, held-writer read-only progress and source exact-end
coverage, with scoped and broader native validation in progress; Compatibility:
public application interfaces and durable fixtures unchanged; Updated handbook:
this milestone page; Hazards and follow-ups: repeat final-source CI and the
unchanged required-host preparation. Complete campaign resources, all faults
and populated restore remain open. No 24-hour run has started.

## Bounded continuous campaign memory observations

The private `CampaignMemorySamples` collector supports the timeline's exact
73–288 five-minute windows with a fixed native process inventory or the exact
replacement roster through `from_roster`. Sampling
once per minute produces at most 1,440 observations over a day; the existing
2,048-record and 4-KiB-per-record ceilings remain. A gap above 75 seconds,
including first and terminal coverage, refuses rather than filling a window
from old data. Completion requires actual terminal clock coverage.

The collector preserves exact process-incarnation and descendant checks and
the explicit whole-process virtual-address-space upper bound. A roster-backed
collector keeps one observation clock, count and coverage array through a
declared retirement and replacement. Prior exit RSS peaks remain attributed
to their roles while current RSS includes only the live owners. Replacement
cannot reset the 75-second gap or erase prior peaks. An untracked successor
still fails the original exact descendant census. Preparation
keeps its existing 1,500-second limit and five-second cadence. Every record
still says `qualifies_resource_model: false`: routing every actual campaign
launch through the roster and independent model reconciliation remain
required. This private collector is a runner prerequisite; it is not yet a
public campaign command or measured campaign evidence.

PR note: Package WP-825 (open); Tier internal; Behavior: bounded continuous
memory observation span; Checks: missing-collector baseline, five campaign
coverage/refusal tests, all 164 lifecycle checker tests and all five scoped
acceptance checks pass; Compatibility: native behavior, fixtures, preparation
limits and qualification thresholds unchanged; Updated handbook: this milestone
page; Hazards and follow-ups: connect the complete fault/resource controller
and repeat final-source CI and real rehearsal. No 24-hour run has started.

The roster integration reaches the missing-owner-interface failure first on
the required host. Twelve roster tests, five fixed-inventory memory tests and
all 212 lifecycle controller tests pass afterward. A further seven-role case
passes with live operator and TypeScript driver owners, their exact retirement
records and both retained exit peaks, under the unchanged 4-KiB record bound.
Final source acceptance remains required; these are component tests, not
measured fault-campaign evidence.

The continuous memory integration subsequently passes all 213 controller tests
and final scoped acceptance on the required host. The next native-observation
component binds counter epochs to the exact roster's server and sampler owners.
A declared restart requires their observed expected exits and exactly one new
incarnation; deployment retains the live provider pool, and restore requires
the next native history incarnation. Component observations retain one global
clock, count, coverage array, byte ceiling and provider peak. Same-lineage
durable frontiers cannot rewind, and restore cannot rewind the previously
observed command head. An independent bounded epoch stream records the boundary
without adding fields to the frozen component evidence. These are controller
prerequisites; actual fault execution and complete resource reconciliation
remain required, and their qualification flags remain false.

The epoch component passes all 229 controller tests and scoped acceptance on the
required host. Native sampler construction now accepts an explicit initial
address-space limit, or routes a successor through the exact campaign roster's
serialized launcher. A competing caller limit is refused before opening output
or launching a process. Closing the sampler retains its exact child object for
the controller's explicit expected-exit retirement. Epoch sampling calls that
same owned sampler with the current release version and checks its returned
elapsed field before recording the unchanged raw native schema. A different
owner, plan identity or timestamp cannot supply evidence for the current epoch.
These connections pass all 233 controller tests and scoped acceptance on the
required host, including a real bounded-parent sampler replacement. Actual
campaign integration and resource-model qualification remain required.

A focused native regression also finds that command-audit lookup can hide a
started audit in the newest private durability-epoch segment: probing its tail
consumes that segment before the fallback searches other members. The repair
keeps the existing tail fast path and searches every private segment for an
exact non-tail member, retaining duplicate-authority refusal and publication
custody. The regression fails at that exact missing-record assertion on the
required host. The positive regression, full storage suite, Clippy, formatting
and scoped package acceptance also pass. Its relationship to the earlier native
audit stop remains unproven.

## Preserve campaign log and administrative child ownership

Continuous native log collection now binds the regular-file inode to the exact
server owner's kernel stderr descriptor. The initial bind excludes preload
records; each replacement starts at byte zero. Before replacement, collection
drains the retired server's complete terminal records only after both old
server and sampler have their observed expected exits. A killed partial tail
remains in its private source file and supplies no observation. A partial
normal-shutdown record, changed inode, symlink, truncation or malformed native
record refuses. The aggregate log ceiling is 16 MiB, with at most 64 lines per
poll and 4 KiB per line. One global clock, coverage, peak and original 75/95-second
observation gaps survive every replacement.

Administrative CLI calls use one serialized roster-owned child in the fourth
reserved auxiliary slot. Each call has a real deadline of at most 60 seconds,
retains at most 1 MiB of stdout and 64 KiB of stderr, and requires the native CLI
schema and command identity with a successful result or the exact native
maintenance-uncertainty protocol. Its actual wait4 exit and peak
memory proof precede the next census, including for a child that already exited
before its first census. Exit 3 is admitted only for backup creation or restore
with an exact `outcome_unknown`, `poll_maintenance_operation` and canonical
operation ID. Every other nonzero exit remains unexpected. Uncertainty requires
polling that original operation; it never reconciles an action or permits a
mutation retry. Output or deadline refusal kills and reaps the owned child; cleanup cannot erase the
original failure. The controller must collect between calls and preserve every
continuous observation gap. This helper alone supplies no maintenance recovery
protocol or action reconciliation.

All 250 controller tests pass on the required host, including actual bounded
parent/child launches, replacement stderr binding, terminal peak retention,
short CLI exits and bounded output refusal. Numeric kernel and diagnostic
fixtures remain component tests. These checks do not establish the real
seven-fault runner, complete resource model or rehearsal readiness.

## Retain the completed campaign load owner until accounting finishes

The actual campaign can explicitly enable `RIFFDB_LIFECYCLE_TERMINAL_BARRIER=1`
with its existing load-control directory and canonical release phase. The load
still offers the same commands and reads, closes each window at its original
deadline, drains under the existing limit and finishes its acknowledgement
witness. It then flushes its unchanged final counts, publishes a bounded
terminal marker and remains alive without issuing further operations. This
lets the controller finish accounting against the original process owner.

The terminal marker contains the current release generation, contract version,
history incarnation and exact completed counts. The controller releases that
same owner through a canonical phase record with the identical credential
reference. The wait is limited to 60 seconds; missing initial phase, stale
markers, changed phase, quiescence, mismatched release, malformed input or an
expired or reversed clock refuse. The marker remains private evidence. The
existing preparation path does not enable this campaign-only handshake.

Deterministic schedules and an actual child-process protocol check publication,
continued ownership before release, exact identity, stale input and deadline
refusal. These are component checks, not proof of a complete resource model or
execution of the real seven-fault campaign.

## Separate durable seal publication from spare preparation

The preparation on `ea572072` still fails `source_bytes_sample_gap`, with no
fresh source-byte record during load. Unpublished refusals stop increasing
during load, but oversized suffix refusals continue. This is a failed
preparation; it does not establish the cause or qualify the host.

The native sealer performs two distinct stages: retire and durably seal the
installed extent, then prepare the next spare. Production spare creation can
zero-fill 320 MiB. Source admission previously tested whether this whole thread
had finished, so completed publication remained pending during future spare
preparation. A deterministic real-checkpoint regression fails that admission
while the worker is paused between the two stages.

Each serialized native job now owns one private completion flag. Only successful
retirement and sealing, including their existing file and directory syncs,
publish it. Source capture and validated admission observe that exact job's
flag without joining the worker. Future spare preparation continues off-gate;
ordinary rotations and shutdown retain the complete thread join. A failed seal
or a successor awaiting its predecessor cannot report completed publication.

The entire published suffix must still fit the 256-transition and 1-MiB drain
limits. Canonical validation, every hold, independent checkpoint evidence,
maintenance pacing, acknowledgement, ordering and durable formats are unchanged.
This is a D-003 implementation choice within ADR-0271 decisions 4–6; it adds
no application control or durable identity.

PR note: Package WP-825 (open); Tier guarantee implementation; Behavior: observe
actual durable seal publication independently of spare preparation; Checks:
failing real-checkpoint baseline and deterministic publication, serialized-job,
native sealing-failure and validated-admission schedules, with scoped acceptance
and broader native validation in progress; Compatibility: application and
durable fixtures unchanged; Updated handbook: this milestone page; Hazards and
follow-ups: final-source CI and the unchanged required-host preparation must
pass. The full campaign/resource model and populated restore remain open.
No 24-hour run has started.

## Ordinary publication handoff for source admission

The diagnostic preparation on `230af6cd` fails the unchanged source-byte sample
gap with no fresh source sample during load. Its last periodic native drain
record reports 276 unpublished and 90 oversized refusals, including seeding;
none report unaccounted metadata. The last refused published suffix contains
403 transitions and 3,633,327 encoded bytes. This failure is preserved. Full CI
on the preceding `3979a181` passes; neither result qualifies the required host.

A deterministic native regression reproduces the publication overlap with a
real submitted audit and its unconsumed original completion fence. The source
turn yields without materializing or publishing it. Previously the next
ordinary writer could not start a source-requested checkpoint through that
unpublished predecessor. The native repair now recognizes an exactly accounted
pending tail or fully published oversized suffix as a scheduling hint,
separately from source admission authority.
It checks the complete frame chain, published boundary, pending counters, frame
identities and existing ceilings; unknown metadata still creates no hint.

One boolean in the existing fixed gate state keeps the ordinary publication
handoff active until an actually drained source capture or graph retirement.
An ordinary writer completes its already submitted FIFO prefix before existing
checkpoint preparation and publishes its own submitted frame before releasing
its current writer lease. Completion tickets retain the exact original results
for their owners. This temporarily changes where ordinary publication waits;
it changes no fsync boundary, commit profile, ordering, acknowledgement, format,
source deletion authority or application interface. A source turn still never
waits for publication and drains only an entirely published suffix within the
existing 256-transition and 1-MiB limits. Ordinary pipelining resumes after the
drained capture; retirement clears both scheduling hints irreversibly.

This is an implementation choice under ADR-0271 and D-003: source admission,
FIFO ticket ordering, bounded drain and maintenance residence pacing retain the
accepted decisions. The ordinary writer already owns publication of the same
submitted prefix through the same durability fence. No partial source drain or
source-owned completion of an unpublished result is introduced.

PR note: Package WP-825 (open); Tier guarantee implementation; Behavior: native
ordinary publication handoff for a bounded source admission attempt; Checks:
failing deterministic publication-overlap regression, exact original completion
ownership, malformed-accounting refusal and retirement schedules, with scoped
checks and broader native validation in progress; Compatibility: durable and
public fixtures unchanged; Updated handbook: this milestone page; Hazards and
follow-ups: actual required-host latency, source progress and full CI on final
source remain required. The complete campaign/resource model and populated
restore remain open. No 24-hour run has started.

## Source progress below the ordinary capacity trigger

A populated required-host preparation passed its first five-minute service
window at 100 commands and 400 reads per second, with p95 29 ms and p99 67 ms,
but failed the frozen 95-second source-byte sample gap. Native counters recorded
538 pending-journal admission yields. The closed source drain admits at most
256 transitions and 1 MiB; ordinary asynchronous checkpointing otherwise starts
at 32,768 transitions or 128 MiB. Continuous published work between those limits
can prevent fresh source barriers even when FIFO source turns are granted.
The failed preparation is preserved and does not qualify the milestone.

For a completely published, consistently accounted oversized suffix, the native
source owner now records one private hint for the next ordinary writer to start
the existing asynchronous checkpoint. The source turn still yields without
draining, rotating or waiting under its lease. Unknown or inconsistent accounting creates no hint. The later ordinary
publication handoff described above also handles an exactly accounted pending
tail without admitting it to a source drain. The ordinary checkpoint retains its
existing limits, canonical validation, publication and crash-recovery protocol;
empty work creates no checkpoint. Source reclamation still requires complete
current authority, actual independent progress and every current hold.

The hint lives in the existing gate's fixed state and disappears on consumption
or irreversible source retirement. Deterministic schedules cover a granted
turn requesting before and after retirement. The real native regression first
acknowledges 257 typed service audits, proves source admission yields without
materialization, then exercises an ordinary asynchronous checkpoint and exact
audit preservation. This repair changes internal checkpoint scheduling within
the accepted decisions; it adds no application control or durable identity.

PR note: Package WP-825 (open); Tier guarantee implementation under ADR-0265,
ADR-0271 and D-003; Behavior: allow ordinary checkpoint progress after a known
source backlog exceeds the closed drain limit; Checks: native failing progress
regression and deterministic retirement schedules, with broader validation in
progress; Compatibility: no command ordering, authorization, source cut,
encoding, dependency or acknowledged-durability change; Updated handbook: this
milestone handoff; Hazards and follow-ups: full checks, real required-host
pressure and the qualifying short campaign remain unproved. No 24-hour run is
started, and the previous failed preparation is not reclassified.

The next populated preparation completed its first five-minute load window but
again failed source-byte coverage. Native source preparation yielded for a
checkpoint while logical retention remained at its target or fenced. The worker
selected its idle pause rather than observing the checkpoint's completion.
Checkpoint installation also precedes asynchronous journal-sealer completion;
the sealer does not announce a second checkpoint. Both failed preparations are
preserved and remain failures.

The actual native regression also found that initial source capture discarded
its admission-yield reason. It now reports the same closed refusal phases as
continuation admission, so the worker can schedule the first capture correctly.
This changes observation and scheduling only; capture still acquires the same
source lease and checks the same publication, epoch and writer conditions.

The worker now uses the actual source-yield phases to wait for the existing
checkpoint and its sealer after completing the entire writer-residence rest.
A journal yield requires an observed native checkpoint; a publication yield
can finish when its existing sealer finishes. Rechecks retain the current
bounded deadline, cancellation and typed refusal. Captured read-only validation
can continue during the wait. A changed checkpoint updates the wait baseline,
so a pending sealer cannot turn the checkpoint condition variable into a spin.
Source-only wakes never grant the logical lane's after-checkpoint drain
exception. Ordinary idle and noncheckpoint refusals keep their prior backoff.

Package: WP-825, open. Tier: guarantee implementation within ADR-0265,
ADR-0271, ADR-0272 and D-003. Compatibility: no new durable identity, command
ordering, authority, source-cut, drain ceiling, dependency or application
interface. Updated handbook: this page. Checks: deterministic source wait,
rest, cancellation and refusal regressions plus actual native suffix and
publication validation; final-source CI and required-host progress remain
required. No 24-hour run is started.

The corrected required-host preparation on `3979a181` still fails source-byte
coverage after completing its first 30,000 commands and 120,000 reads. Shutdown
records 397 pending-journal, 24 checkpoint-publication and 40 page-bound yields,
with one actual recycled generation. The first window overlaps the prescribed
sink outage; its latency overflow does not establish normal-window latency.
The run remains a preserved failure, and its populated restore is not launched.

To diagnose the remaining admission failure, the native worker emits the fixed
`riffdb-retention-source-drain-v1` diagnostic at its existing 60-second cadence.
It separates cumulative unpublished, unaccounted and oversized refusals and
reports eight numeric fields from the latest refused suffix. Unpublished means
that at least one native unpublished counter was nonzero; it does not certify
consistent accounting. Optional observation failure reports `available=0`,
never a zero backlog. Equality bits disclose publication agreement without
exposing hashes or payloads. Counters saturate, and unrepresentable diagnostic
sizes use the unsigned maximum sentinel. The fixed gate-owned state adds no
waiter, thread, checkpoint hint, source cut, mutation permission or drain work.

Package: WP-825, open. Tier: guarantee diagnostic implementation under ADR-0271
and D-003. Compatibility: the exact admission predicates, hint rules, writer
rest, public interfaces and durable formats remain unchanged. Updated handbook:
this page. The native regression checks all three refusal classes without
changing durable state or granting checkpoint work to unknown/unpublished
accounting. Actual required-host diagnosis and repair remain necessary.

## Persistent child launch and exit observations

The preparation now retains a private, exclusive usage record for each server,
load worker, event observer, native sampler and language driver incarnation.
Polling, joining and signaling share one child owner. Reaping always uses Linux
`wait4` so ordinary liveness polling cannot discard that child's kernel RSS high
water. Restart records belong to separate owners and files. A timeout keeps the
child owned until the existing exact-descriptor kill and bounded join complete;
it still fails the preparation. Failed launch, unavailable launch memory and
descriptor admission failure remain explicit unknowns.

The record contains numeric observations and closed labels, without arguments,
process identities, paths or credentials. Final allocator peak remains unknown.
These observations do not prove every descendant's lifetime, simultaneous
aggregate peak or the complete campaign resource model. Existing live inventory,
service thresholds and fail-closed qualification checks remain required.

PR note: Package WP-825 (open); Tier internal; Behavior: preserve persistent
owned-child launch and exit RSS evidence across polling and restart; Checks:
actual allocation, timeout/kill, independent restart, concurrent wait/signal,
invalid usage, failed launch and descriptor-refusal regressions; Compatibility:
no database, application, durable format or dependency change; Updated handbook:
this milestone handoff; Hazards and follow-ups: final allocator and complete
descendant accounting and the actual qualifying campaign remain unproved.
No 24-hour run is started.

The target is 65,536 seeded tickets across 64 organizations, 100 commands/s
plus 400 named reads/s, at most 32 in flight, and recovery to the first
successful write within 60 seconds. Qualification uses an inventoried 8-vCPU,
32-GiB Linux host with durable SSD storage. Normal-window p95 is below 100 ms
and p99 below 250 ms. Existing stricter applicable gates remain effective.

## Decisions and dependencies

The maintainer accepted these exact records with "Accept all three as written":

- Storage lifecycle (`adr/0265-sustainable-ticketdesk-storage-lifecycle.md`):
  production local delivery, receipt reclamation, validated-prefix refresh,
  custody, and a separately identified 24-hour rehearsal.
- Provider retirement (`adr/0266-bounded-exact-provider-registration-retirement.md`):
  existing 256-slot caps plus a 1-GiB aggregate ownership budget, exact policy
  identities, protected epochs, and reclamation after final reference release.
- Ordering boundary (`adr/0267-ordering-boundary-for-the-sustainable-application-milestone.md`):
  preserve today's global sequence; partition locality does not discharge R-13.

| Package | Deliverable | Hard dependencies |
|---|---|---|
| WP-821 | Exact architecture acceptance and evidence contract | WP-814, WP-815, WP-817 |
| WP-822 | Durable local archive and production outbox worker | WP-821 |
| WP-823 | Receipt/extent reclamation and retention-aware recovery | WP-821, WP-822, WP-815 |
| WP-824 | Exact-provider retirement and byte accounting | WP-821 |
| WP-825 | TicketDesk workload and actual campaign runner | WP-822, WP-823, WP-824 |
| WP-826 | Real uninterrupted 24-hour qualification | WP-825 |

Run `./scripts/brief WP-821` and the corresponding downstream brief before
implementation. Architecture acceptance is recorded in the separate governance
commit `d6d5dd5b`. Package closure still requires its own validation and evidence;
acceptance alone does not establish implementation or campaign results.

## Findings carried into implementation

- `FileTestConnector` only calls its injected writer's `flush`; it cannot
  establish durable destination acceptance. WP-822 adds a separate redb archive
  with Immediate two-phase publication and exact event deduplication. The
  fixture connector remains test infrastructure.
- The source-control reclamation method is currently private, with retained
  observations exercised by fixtures. Production needs a closed owner that
  survives independent checkpoint observations without creating command-path
  drains or using reclamation's own receipt as perpetual progress.
- Online pruning invalidates validated-prefix evidence. Fresh validation must
  establish a new binding; retaining a deleted certificate is forbidden.
- Exact-provider keys include policy/capability revisions. Increasing slot
  ceilings or sharing similar-looking policies does not solve lifetime ownership.
- The existing alpha endurance validator requires its full domain/language
  matrix. A TicketDesk rehearsal cannot masquerade as that release campaign.

The ordering review covers two independent routes observed through one current
global token, authority/catalog changes spanning owners, rejection of a
cross-route write, recovery of a committed command whose reply was lost, and
fencing an old owner before promotion. No new token, route counter, quorum,
or distributed implementation is authorized by this milestone.

The latest 1,500-second preparation meets sink catch-up but fails normal-window
latency and finishes some requests after the load deadline. A stopped-source
private copy passes complete archive reconciliation, then fails to reach
readiness within 60 seconds after pruning. Prefix-refresh setup is repaired
to release the ordered writer before population work while preserving exact
binding and stale-candidate refusal. These native blockers still require
measured resolution; component tests do not establish rehearsal readiness.
See the [actual observations](WP-825-LIVE-OBSERVATIONS.md#extended-preparation-and-populated-recovery-blockers).

## Evidence checker

Run the hostile-evidence tests with:

```bash
./scripts/check-ticketdesk-lifecycle --self-test
```

The tests construct synthetic inputs only in memory or temporary directories.
They prove refusals and checker behavior, not actual delivery, reclamation,
recovery, resource use, or throughput. They are wired into `scripts/ci-all`.

The preparatory verifier accepts a frozen manifest and a receipt:

```bash
./scripts/check-ticketdesk-lifecycle \
  --manifest ./evidence/manifest.json \
  --manifest-sha256 "$FROZEN_MANIFEST_SHA256" \
  --receipt ./evidence/receipt.json
```

Freeze the manifest digest before the run; do not recalculate it to excuse
changed targets or artifacts. Both JSON files have a 4-MiB bound, closed
fields, exact schemas, duplicate-key rejection, and integer-only counters.
Artifact paths are relative to the receipt directory; traversal and symlink
components are refused, and each regular artifact is streamed with a 1-GiB
bound and checked against its frozen digest.

The receipt identifies itself as measured rehearsal evidence and has 288
contiguous five-minute windows. Planned faults and actual actions match;
five-minute outages have at most ten minutes to recover, and restart recovery
including the action itself has at most 60 seconds. The last six hours have no
fault interval and no positive eligible-history trend or endpoint growth. Physical
reclamation requires three distinct extent generations. Resource ceilings are
frozen separately per resource; archive growth cannot excuse process-memory
growth. Missing samples, unexplained errors, interference, unaccounted retries,
artifact drift, and an alpha classification refuse evidence.

This is an evidence-contract verifier, not an attestation service. Labels and
booleans cannot establish that measurements happened. WP-825 still owns the
real runner, independent state/resource reconciliation, trustworthy collection
of action and artifact identities, and tests of that producer. No receipt is
eligible for WP-826 closure until those producer checks and every prerequisite
have passed. No campaign receipt is included in this change.

### Native observation preparation

WP-825 adds a persistent Rust `lifecycle native-observer` worker that uses the
existing verified TLS client and an operator credential. Its closed stdin
protocol accepts only `sample 1`, `sample 2`, or `stop`, each followed by a
newline. A sample performs one health, statistics and one-row projection call
sequentially in one auxiliary slot, with no retries and a five-second total
deadline. Output contains numeric measurements and the exact projection plan
identity, with no event payloads, credentials or process identifiers. The worker
refuses missing counters, changed incarnations, unavailable projections and
substituted plan/frontier identity. It remains preparation code until integrated
and measured with the final campaign controller.

The production statistics provider previously always omitted delivery counts.
It now reads the cardinality of the existing validated, commit-coupled
undelivered index on its bounded read worker. Claims, retries and dead letters
remain counted; delivery completion removes membership. Unbuilt or degraded
membership remains unavailable. This adds no durable record, population walk,
writer, deletion authority or authorization exception. See
[live delivery counts](../operations/LOCAL-OUTBOX-ARCHIVE.md#live-delivery-counts).

The owned-process collector verifies all thread child lists against an exact
tree of named persistent owners. It refuses untracked children, replacement,
disconnected ownership and missing memory measurements. It records current
RSS/address space and per-incarnation lifetime high-water marks; the sum of
lifetime peaks is a conservative upper bound, not a simultaneous measured peak.
Transient descendants between censuses still require launch/exit accounting.
These collector tests do not establish the campaign resource-model check.

An enumerated Linux thread can retire before its child-list file is opened
while the process remains live. The collector verifies that the task itself
disappeared, then restarts the complete incarnation and memory census, with at
most three passes and no wait or sleep. It preserves every child already seen
and every observed high-water mark across those passes. A vanished process,
replacement, missing child-list file on a live task, lost child evidence or
exhausted attempt bound still refuses the observation. Owned-process samples
disclose cumulative `task_census_restarts` separately from memory byte totals.
This correction does not establish transient launch/exit coverage or qualify
an earlier failed preparation.

Package: WP-825, open. Tier: guarantee under accepted ADR-0265 decision 11.
Behavior: native delivery-count observation and preparatory public-client/process
collection. Compatibility: existing statistics fields and capability checks;
durable formats and command ordering are unchanged. Checks: 88 lifecycle
checker tests, native mixed-state/reopen and completion tests, a deterministic
durable-but-unpublished event schedule, and scoped Clippy pass. Final source
acceptance, full CI and actual observer host integration remain required; no
qualifying rehearsal is claimed.
Updated handbook pages: this handoff and local archive operations. Follow-ups:
integrate the observer, inventory transient lifecycle processes, measure eligible
history bytes, and complete all seven fault paths with six final fault-free hours.

## Persistent native observation in population preparation

`ticketdesk-lifecycle-smoke --native-observations` selects the persistent Rust
health/statistics/projection sampler for an observed population preparation.
It requires `--observe` and a supported positive `--population-probe-seconds`.
The preparation operator receives the existing `read_statistics` capability;
application credentials and public authorization behavior are unchanged.

One reserved auxiliary slot issues three sequential public RPCs per successful
sample, with no retry. The controller checks the exact projection identity,
contract selector, sample sequence, history incarnation and monotonic frontiers.
Missing output, a changed identity, partial RPC failure or expired child shutdown
refuses the preparation. Samples retain actual delivery counts and projection
lag. The existing five-second cadence and every-window coverage check remain.

Memory collection includes the native sampler in a verified five-process
persistent tree and records lifetime peak upper bounds. It still sets
`qualifies_resource_model` to false: transient CLI launch/exit accounting,
eligible-history bytes, fault integration and final-source host validation
remain required. This preparation does not establish a qualifying campaign.

Package: WP-825, open. Tier: internal. Behavior: connect the native sampler to
population preparation while preserving load, latency and refusal gates.
Checks: closed observation/continuity tests, actual child ownership and shutdown
tests, and all 98 lifecycle checker tests. Compatibility: no native interface,
durable format, application permission or fixture changes. Updated handbook:
this page. Scoped acceptance: all five checks pass. The first local population
preparation failed before sampler startup: the event observer refused a consumer
acknowledgement during seeding, and cleanup exceeded its shutdown bounds.
That failed run is diagnostic evidence only; its unoptimized binaries and local
host do not qualify performance. A separate small public TLS smoke successfully
used the real native sampler for health, statistics and the compiled projection,
then joined it normally; this does not prove population-load sampling.
Hazards/follow-ups: identify the acknowledgement
result, measure real public RPCs and the five-process census together, then run
final-source full CI and required-host preparation.

## Closed acknowledgement refusal diagnostics

The event observer preserves the distinct public mutation result in its closed
failure code: changed state, missing consumer, outstanding lease, stale lease,
or expired lease. Only an applied acknowledgement permits continued observation.
It does not retry, lengthen leases or accept a refused acknowledgement as success.
No event identity, lease token, payload or credential enters these diagnostics.

Package: WP-825, open. Tier: internal. Checks: the generic-cause regression fails
before the correction; all 28 lifecycle-worker tests pass. Compatibility: public
RPCs, lease timing, durable formats, credentials and workload gates are unchanged.
Updated handbook: this page. Follow-up: use the actual result to diagnose the
failed population seed; it is not an established root cause or native repair.

## Driver startup boundary in full CI

The frozen candidate passed all 20 scoped checks, including 2,862 native tests,
but full CI failed in the nested retained TypeScript smoke. Its owned temporary
root produced a 110-byte socket pathname; Linux accepts at most 107 bytes.
The driver refused startup, and the cleanup exception replaced that primary
failure with `driver_shutdown`. Both required-host launch guards refused; no
host workload or 24-hour run started.

Application and operator drivers now preflight addresses through the standard
library's platform Unix socket address constructor, before artifact reads,
remote connections or filesystem mutation. Typed refusals and private-directory
ownership checks remain. Preparation uses a shorter owned temporary directory
name and retains the primary failure while separately recording failed cleanup.
A successful smoke still fails if cleanup refuses or exceeds its deadline.

Package: WP-825, open. Tier: surface; implementation repair under ADR-0106,
existing safe configuration boundaries and D-003. Checks: native path-limit
regressions fail before the repair; all 44 native driver tests, cleanup refusal
tests, all 100 lifecycle checker tests and nine scoped acceptance checks pass. Compatibility: no dependency, protocol, durable format,
credential or fixture change. Updated handbook: this page and Driver Host.
Follow-ups: repeat actual nested four-language preparation, then complete
final-source full CI and required-host validation.



## Exact physical retirement witnesses

After native eligibility and reader checks, namespace retirement and successful
parent-directory sync, the native owner emits one closed numeric
`riffdb-retention-extent-retired-v1` record naming its publication root,
physical generation and deletion or recycling action. A held or failed pass
emits no successful retirement witness. The record grants no deletion authority
and changes neither the existing eligibility checks nor the writer lane.

The preparation collector validates at most 4,096 witnesses in its existing
bounded, immutable shutdown log. Within that one owned daemon incarnation it
refuses duplicate generations, rewound publication roots, unknown formats,
missing witnesses, mismatched deletion/recycling totals, still-present sampled
files or sampled disappearances of a different generation. Repeated shutdown
censuses are validated as copies, never added. Exact native identities are
reported separately from the periodically sampled file inventory, which can
miss a file's entire lifetime. Neither view establishes eligible allocated
bytes, complete resource accounting or 24-hour qualification.

Package: WP-825, open. Tier: guarantee, implementation evidence under ADR-0265
and D-003. Compatibility: no public interface, durable encoding, authorization,
ordering, dependency, fixture or threshold change. Updated handbook: this page.
The native barrier-release regression first exposes the missing witness;
collector hostile tests cover exact counts and generation correlation.
Final-source acceptance and actual required-host evidence remain required.
## Separate physical-byte collection during observed load

Observed online-retention load with native observations collects the complete
`riffdb-retention-physical-bytes-v1` component alongside source and logical byte
components. The bounded daemon-log reader preserves allocated-block values,
reference and reader protection categories, original generation cutoff and the
explicit exclusion of the diagnostic pin. The collector checks byte conservation,
sector units, extent/page bounds, native root/incarnation continuity and the
30-second capture lifetime. Each five-minute window needs complete observations
with the same frozen 95-second maximum gap as the other byte components.
Unknown, partial, malformed, expired, refused or replaced observations fail.

These original-prefix allocated bytes remain separate from resolved canonical
logical/source bytes, active/spare files, retained evidence, archive, backups and
derived files. Adding overlapping representations or allowing one component's
fall to conceal another's growth would not establish the aggregate model.
Neither these samples nor their summary fills `eligible_history_bytes` or
qualifies complete resources. The matching native producer must accompany the
collector before required-host use.

Package: WP-825, open. Tier: internal. Compatibility: no public interface,
durable encoding, authorization, dependency, reviewed fixtures or threshold
change. Updated handbook: this page. Six focused semantic collector tests and
all 120 lifecycle checker tests pass. Required follow-ups are native record
integration, complete independent resource reconciliation, actual collection
pressure and final-source validation; no qualification is claimed.
## Original sealed-prefix physical byte observations

A native diagnostic captures the materialized reference roots, current
publication and active-generation cutoff when the writer is idle. It walks the
original sealed prefix through the existing media cursor, inspecting at most 64
directory entries and fully validating at most one bounded extent per advance.
Every eligible file uses the native watermark, source resume, incarnation and
exact absent-reference checks. Kernel allocated blocks are counted separately
from logical file length; simulated logical lengths never become allocated-byte
evidence. Invalid, incomplete, oversized or expired observations remain unknown.

The census is bounded to 8,192 directory entries and 4,096 extents, with a
30-second capture lifetime and at least 60 seconds between successful captures.
A diagnostic scrub pin preserves original immutable files during the walk.
Other original reader/scrub holds and native references are accounted separately.
The observer explicitly excludes its own temporary pin from the otherwise
eligible byte count; this is a diagnostic of the original prefix, not current
retirement authority. New generations at or above the captured active cutoff,
the active file, spare, archive, backups and derived files belong to separate
resource components. This record cannot stand in for a complete file inventory.

The native lane ticks this observer during ordinary retention steps as well as
checkpoint waits, before writer admission. An idle or progressing lane therefore
does not depend on a checkpoint stall to begin or continue its census. Both
paths share the same observer, capture cadence, page limit and retirement owner.

A populated host preparation exposed a scheduling defect: one physical page per
ordinary turn could expire the census while the worker slept between turns.
The worker now continues already captured read-only pages during its idle and
writer-residence rest, using the existing 100-ms cancellation slice. Page time
counts toward the same rest clock; completion stops page work and waits the
remaining rest. Stop and explicit wake retain their existing behavior. This
continuation cannot capture a new census or start a writer turn. Checkpoint
waits continue physical pages even when source-receipt validation is complete.
The 30-second capture lifetime, 60-second capture cadence, complete-prefix
validation and all file/reference checks remain unchanged. The failed host
preparation remains failed and cannot qualify a later source revision.

Completion, expiry, refusal and joined worker retirement release the diagnostic
pin, engine snapshot and directory capability. `riffdb-retention-physical-bytes-v1`
reports the original root, incarnation, watermark, resume, cutoff, complete
extent count, eligible and protected allocated bytes, protection categories,
reader hold, capture age and pages. Refusal invalidates measurement while native
pruning and retirement retain their independent checks. There is no new writer
admission, deletion authority or public observation surface.

Package: WP-825, open. Tier: guarantee, implementation diagnostics under ADR-0265
and D-003. Compatibility: no public interface, durable encoding, authorization,
ordering, dependency or reviewed fixture change. Updated handbook: this page.
The real native barrier-release regression first exposes missing measurement,
then covers reader/scrub/writer/source holds, exact allocated bytes, complete
empty prefix and release before probe-object drop after completion, expiry or
census-bound refusal. Collector integration, independent aggregate modeling,
full final-source checks and actual required-host pressure remain required.
This component does not fill `eligible_history_bytes` or authorize qualification.

The native barrier-release fixture also drives the production physical-byte
emitter before retirement. Independent collector parsing of its actual records
checks the real diagnostic boundary, including allocated bytes, original
eligibility and pin release. It remains local fixture evidence rather than a
populated required-host qualification.

## Independent final-period history component checks

The independent reconciler revalidates the three native collectors' exact
schemas, bases, accounting, freshness, continuity and offered-clock coverage.
It preserves separate canonical logical history, canonical source receipts and
original sealed-prefix allocation. Each final five-minute window reports its
observed maximum for each component. All 72 final quiet-period windows must
be present, and each component independently passes both the endpoint and
least-squares trend checks. Declining logical bytes cannot hide increasing
source or physical backlog; a final cleanup cannot hide growth across the
period. Missing data never becomes zero.

Physical allocation overlaps canonical representations, so these quantities
are never added into an aggregate eligible-history scalar. This reconciler
retains `qualifies_complete_resource_model: false`. It cannot supply live data,
retained outcomes/provenance, protected history, memory, archive, backup or
derived-state accounting, original artifact custody or actual fault evidence.
The campaign must supply those independently before qualification. The
`scripts/check-ticketdesk-lifecycle-history-components` checker reads only the
three closed private sample files. Its bounded reader refuses symlinks,
hardlinks, duplicate JSON fields, truncation, nonprivate permissions and a
changed input cohort. Its separate output binds each input's SHA-256, byte
length and record count and cannot overwrite an earlier report. Local
synthetic tests establish arithmetic and refusal semantics only.

Package: WP-825, open. Tier: internal under ADR-0265 decisions 15 and 16.
Compatibility: no public interface, durable encoding, authorization, ordering,
dependency, reviewed fixture or existing qualification-threshold change.
Updated handbook: this page. Actual final-source campaign integration and
complete independent resource qualification remain required.

## Actual logical-history byte observations

The native online lane can emit `riffdb-retention-logical-bytes-v1` after
finishing the complete eligible prefix of one original composite snapshot.
It uses the native retained-sequence target and fencing inputs, checks actual
delivery in that snapshot, and stops before protected or unsealed whole rows.
A 256-row scan page is incomplete evidence until the original eligible end is
known; it is never reported as the total backlog.

The value count covers resolved canonical command rows, their prunable event,
outbox and status rows, and obsolete command locators. The key count covers
removed keys only. Retained outcomes, provenance, audit, request indexes and
event routes stay in their separate resource model, including compatible
rewrites of retained full records. The accounting adapter executes the existing
native retained-view validation against private bounded staging; it changes no
database row and confers no deletion authority.

Capture occurs off the writer gate at most once per 60 seconds. Each page
visits at most 256 command rows, with the existing journal-frame byte and
collection bounds on per-row related work and staging. A captured pin expires
at 30 seconds, and completion, refusal and joined worker retirement release it.
Incomplete, expired or failed measurements remain unknown. A closed
`riffdb-retention-logical-bytes-refusal-v1` diagnostic invalidates accounting
without replacing or changing the native pruning plan.

This is a separate diagnostic component. It does not fill
`eligible_history_bytes`, qualify the aggregate resource model, or establish
physical retirement. Unit fixtures cover canonical byte sizes, retained views,
the original prefix beyond one deletion batch, operator holds, actual delivery,
target boundaries, corruption, workspace limits, overflow and pin lifetime.
Required follow-ups are collector integration, comparison with real native
prune and extent retirement, and measurement pressure on the required host.

Package: WP-825, open. Tier: guarantee, implementation under ADR-0265 and D-003.
Compatibility: no durable encoding, application interface, authorization,
command ordering, dependency or reviewed compatibility-fixture change.
Updated handbook: this page. All 10 focused native accounting tests pass;
full scoped checks and final-source host evidence remain required. This
component authorizes no 24-hour qualification.

The actual native two-command recovery fixture now compares the logical byte
observation with independently enumerated row changes from a real online prune.
It resolves removed command rows canonically, counts removed keys and obsolete
locator values separately, and checks that the delivered eligible prefix is
empty afterward. The same fixture preserves the exact original outcomes and
provenance through the prune and subsequent outbox transition. This is native
fixture evidence; full-population collection pressure and the aggregate resource
model still require host measurements. The measurement entry is test-only and
changes no application or runtime interface.

## Separate logical-byte collection during observed load

Observed online-retention load with native observations also collects
`riffdb-retention-logical-bytes-v1` into private logical-byte samples and an
observation summary. It reuses the bounded daemon-log reader, checks the closed
numeric record and native 256-command whole-row bound, and binds samples to the
independently observed incarnation. Protected rows may lower the eligible end;
the original durable head and watermark cannot rewind. Unknown, partial,
expired, malformed or refused observations fail the preparation instead of
becoming zero. Each five-minute window needs complete observations, with the
same frozen 95-second maximum gap as source accounting.

Logical observations preserve canonical value bytes and removed-key bytes
separately. They do not substitute for source bytes or physical allocated extent
bytes, fill `eligible_history_bytes`, or qualify the aggregate resource model.
The native logical producer must accompany this collector before host use.

Package: WP-825, open. Tier: internal implementation and acceptance under
ADR-0265. Compatibility: no public interface, durable encoding, authorization,
dependency or reviewed compatibility-fixture change. Updated handbook: this
page. Six focused collector tests pass; native and required-host evidence remain
required. This component authorizes no 24-hour qualification.
## Transient invocation exit observations

Preparation invocations now preserve a private bounded launch/exit observation
alongside each invocation's output. The controller reaps that exact owned child
with Linux `wait4`, retaining its per-child kernel RSS high water and exit status
even when its whole lifetime falls between persistent-process samples. Timeout
still fails after killing through the exact process descriptor and joining the
child. A colliding observation refuses before launch; a failed launch remains
unknown. Command arguments, process IDs, names and paths are not serialized.

A bracketing live launch reading preserves the virtual-address and RSS counters
when available. An already exited child can have no live launch reading. Final
allocator high water remains unknown, rather than inferred from RSS or replaced
by zero. These invocation records do not prove complete descendant ownership,
persistent-process exit peaks, final allocator accounting or the aggregate
campaign resource model. The separately owned server, load, observer, driver and
native sampler still require their own launch/exit integration.

Package: WP-825, open. Tier: internal. Compatibility: no native semantics,
public interface, durable format, dependencies, reviewed fixtures, workload or
qualification threshold change. Updated handbook: this page. Real child tests
cover a between-sample 32-MiB peak, timeout and joined exit, invalid usage after
reaping, failed launch and collision. Required-host preparation and complete
process accounting remain required; no qualification is claimed.

## Linux RSS counter observations

A preparation failed closed at resource collection. A bounded allocation-only
host reproducer, with no database workload, also reached the collector's
high-water monotonicity refusal. Linux's RSS accounting is asynchronous; its
reported high-water counter can decrease between reads. The collector now
preserves each raw reported reading and the maximum already observed for the
same live process incarnation. Both bracketing tree passes retain their raw
values. The observation explicitly labels the asynchronous RSS basis.

Virtual-address high water still cannot rewind. Missing, malformed, oversized,
inconsistent, replaced-process or unowned-child measurements still refuse.
Neither the workload nor any latency, recovery, memory or coverage limit changes.
Reported RSS counters remain approximate; this repair does not establish precise
RSS, complete transient launch/exit coverage, or campaign resource qualification.
See the [Linux process accounting documentation](https://docs.kernel.org/filesystems/proc.html).

Package: WP-825, open. Tier: internal. Compatibility: no database semantics,
public API, durable format, authorization, dependency or reviewed compatibility
fixture change. Updated handbook: this page. Preserve the failed preparation;
repeat unchanged-host preparation after the repair's checks, and investigate any
remaining failure rather than assuming the original refusal had this cause.

## Actual source-receipt byte observations

The legacy read-only source observer can report `riffdb-retention-source-bytes-v1` after
completely scanning the actual uncapped eligible prefix from its validated
barrier. It counts resolved canonical receipt bytes and actual key bytes
separately. A sequence gap, padded journal frame length or the next 256-row
reclamation transaction is never substituted for the complete byte count.

Capture retains the original registered checkpoint root and native ceiling,
including the independently durable checkpoint and existing derived/durable
holds. Every advance scans at most 256 rows within the existing changelog frame
byte ceiling, outside the writer gate. Collection performs no mutation, journal
drain, new reader capture or extra writer admission. It leaves source and logical
reclamation decisions intact and never confers deletion authority.

Capture is limited to once per 60 seconds. Collection checks a 30-second
freshness deadline before each bounded page and again before publication;
expired or incomplete work emits no successful byte measurement. Completion,
expiry, failed decoding, owner retirement and switching to offline source control
release the registered reader pin. Offline source control does not advance an
online observation, so it must retire that observation before acquiring its barrier.
An in-progress collection can delay physical reclamation, so the final campaign
must account for this pressure. Storage I/O uses the existing bounded page
operations; the deadline does not cancel an I/O operation already in progress.

Output includes original root, incarnation, resume and ceiling, exact row count,
canonical receipt and key bytes, capture age, collection time and page count.
This is one component of history accounting. It does not qualify total eligible
history: logical command/event/outbox rows and physically eligible extents still
need independent native measurements, with protected resources counted separately.

Package: WP-825, open. Tier: internal; implementation diagnostics under accepted
ADR-0265 decisions 8–11 and D-003, without a durable identity, application surface
or guarantee change. Checks: unknown-prefix regression fails before implementation;
the initial six source-byte tests and all 45 selected source-control/extent
regressions pass. That revision passes all seven scoped acceptance checks,
including 1,245 tests across eight binaries with two existing skips. A subsequent
offline-switch regression reproduces a retained metric pin; the repaired switch
passes all 15 source-control tests, including all seven byte-observation tests,
and all six final scoped non-test checks, including all-target Clippy and the
handbook check.
Updated handbook: this page.
Follow-ups: complete aggregate native byte accounting, observe actual collection
pressure and run final-source full CI.

A separate local release-mode population diagnostic seeded 65,536 tickets,
completed 500 updates and 2,000 reads without retry or late work, reconciled all
tickets, and sampled real public RPCs with a verified five-process memory census.
It failed the existing latency gate: p95 was 105.306 ms, above 100 ms; p99 was
157.170 ms. Archive reconciliation was not reached after that refusal. Its
source-receipt byte collector was absent. This failed five-second diagnostic on
the local host is not required-host or campaign performance qualification.

## Source-byte evidence during observed preparation

Preparations with online retention and native observations now collect the
native `riffdb-retention-source-bytes-v1` records after the load-ready boundary.
They preserve canonical receipt and key bytes separately, original root and
uncapped range, row count, collection time and capture age. The independently
sampled public history incarnation must match. Seed-time lines cannot fill load
coverage; a partial line stays pending, and replaced or oversized logs refuse.

Every five-minute window requires a complete measurement. Collection refuses
missing observations and gaps above 95 seconds: the native 60-second capture
cadence plus strictly less than 30 seconds to complete, with five seconds for
collector scheduling. This is a new component bound; existing provider and
projection coverage limits remain intact. Native stale or incomplete scans
cannot be reported as zero, and unknown diagnostic versions refuse.

This component never fills `eligible_history_bytes` or qualifies the complete
resource model. Logical command/event/outbox history, physical extent
eligibility, process lifetime accounting and real collection pressure remain
required. Collection adds no RPC or sampling thread and reuses the bounded
diagnostic log reader. It does not change native reclamation or admission.

Package: WP-825, open. Tier: internal. Decisions: component evidence under
ADR-0265 and D-003, with no new database surface or guarantee. Checks: collector
regression fails before implementation; all 106 lifecycle checker tests and all
five scoped acceptance checks pass. A real native record from the source-control
regression also parses with its original receipt and key counts. Updated handbook:
this page. Follow-ups: integrated final-source CI, real observed preparation and
complete campaign accounting. Synthetic checker tests are not host qualification.


## Concurrent outbox health refreshes

A deterministic native regression reproduces overlapping dispatcher/commit
refreshes: an older empty snapshot finishes after a newer nonempty snapshot
and incorrectly clears degraded status. Health is a cached diagnostic; a
separate health/statistics sample crossing new commits does not itself prove
this race or failed sink catch-up.

The native cache now gives each refresh an ephemeral generation and marks its
in-progress result degraded. Completion uses an exact-generation compare and
exchange, so an older result cannot overwrite a newer snapshot. Generation
exhaustion remains permanently degraded and cannot wrap or be cleared by the
last delayed observation. Destination readiness stays independently checked.
No new lock spans repository work; unknown reads still become degraded.

Package: WP-825, open. Tier: internal. Behavior: preserve conservative native
health under overlapping refreshes. Decisions: implementation repair under
ADR-0265 and D-003; no delivery, command ordering, authoritative mutation,
durable format, authorization or RPC schema change. Checks: explicit-channel
schedule fails on the original implementation; all six native cache tests,
all 647 server tests across 17 binaries with eight existing skips, and all seven
scoped acceptance checks pass. Updated handbook: this page. Follow-ups: validate
the integrated source in full CI and required-host preparation.

The socket-preflight candidate separately passed the complete actual lifecycle
preparation on its clean revision: nested retained TypeScript, Go and Python,
Rust workload, additive deployment, graceful restart, process kill, archive
reconciliation and capability rotation. That repair removes the confirmed
110-byte-path blocker; it does not turn the earlier full CI failures into passes.
The source-byte producer, collector and health-cache repair are now integrated
for final-source validation. Full CI and required-host qualification remain pending.


## Current handoff

Package: WP-821; tier: guarantee. This preparation adds accepted decisions,
dependency-ordered packages, and internal evidence-validation tests. Existing
database/application behavior and formats have not changed. The handbook page
is linked for review and explicitly distinguishes planned behavior from implemented behavior.

Validation on October 1, 2026: all 20 evidence-checker tests pass, and
`./scripts/acceptance --wp WP-821 --range HEAD..HEAD` passes all 11 selected
checks, including formatting, handbook build/links, requirement coverage,
ADR obligations, index, acceptance-commit discipline, and allowed paths.
Every downstream package brief resolves. `bash -n scripts/ci-all`,
`git diff --check`, and the full `./scripts/ci-all` battery also pass. WP-821
is complete. WP-822 is also complete as recorded below; WP-823 through WP-826
remain open. No real rehearsal has run.
Full alpha qualification, broad epoch reset, driver replacement, remote webhooks,
distribution, and additional micro-optimization remain outside this milestone.

## WP-822 implementation and PR note

Package: WP-822. Tier: guarantee, implementing accepted ADR-0265 decisions 2–7.

Behavior added: an optional primary-only durable local archive, protected
restart-applied configuration, and a server-owned recovered dispatcher. Real
compiled commands emit events that the worker archives with atomic encoded-byte
accounting. Equal uncertain retries preserve the original record; unequal bytes
are corruption. Known custody or quota refusal pauses before a claim, while
post-claim failure preserves existing bounded retry/dead-letter semantics.
Command acknowledgement does not wait for destination I/O. Shutdown cancels
between bounded recovery pages and joins the worker before writer close.

Compatibility: the new record has a sealed destination registry, a separate
Protobuf source, and a stable compatibility vector. Primary registry membership
and record schema hashes are unchanged. No-destination behavior, compiled writes,
authorization, idempotency, provenance, and database-wide ordering are preserved.
Followers reject destination configuration. Local inspection requires Linux and
exclusive engine custody; it exposes no payloads or application RPC/MCP method.

Checks run: `./scripts/ci-all` passes, including workspace tests, clippy,
documentation, dependency checks, four-language operational conformance,
generated fixtures, release source installation, and existing recovery gates.
`./scripts/acceptance --wp WP-822 --range HEAD..HEAD --no-tests` passes all 19
selected non-test checks; tests ran in full CI. The corrected exact schema
inventory test also passes independently. Process tests cover four destination
commit/publication cuts with repeated restart, source-completion uncertainty,
quota, permission recovery, corruption, real production delivery, original
command replay, and conflicting idempotency input. `git diff --check` passes.

Implementation choices: recover 256 rows/4 MiB per archive step; reserve a
3-MiB outer record payload bound and 8-MiB engine cache. Linux custody pins
directory/file identities and verifies effective-user ownership, private modes,
symlink refusal, and one hard link. Inspection reports incomplete until exact
end and caps each invocation at 256 pages. Generators own all package assets,
format/fixture digests, adapter templates, and CLI reference changes.

Updated handbook pages: [local archive operations](../operations/LOCAL-OUTBOX-ARCHIVE.md),
[configuration](../configuration.md), [domain events](../contracts/DOMAIN-EVENTS.md),
[CLI reference](../reference/CLI.md), and this milestone handoff; `SUMMARY.md`
links the new operations page.

Hazards and follow-ups: archive rows are intentionally retained, with no automatic
deletion; quota excludes separately measured engine allocation and cache memory.
Dead letters continue to fence source history. WP-823 owns repeated receipt and
physical extent reclamation plus retention-aware recovery. WP-824 owns provider
retirement, and WP-825/WP-826 own the real workload and uninterrupted rehearsal.

## WP-823 source-reclamation implementation and PR note

Package: WP-823, still open. Tier: guarantee, implementing accepted ADR-0265
decisions 8–10 and the receipt portion of decision 11.

Behavior added: the production retention lane retains one source-control owner
across checkpoint observations. Complete immutable history and source evidence
are validated in bounded pages outside the writer gate. Only exact-end proof
can request the bounded FIFO source turn accepted in ADR-0271; earlier
writers retain precedence. One fixed request expires after 100 ms, and graph
retirement cancels pending eligibility. An in-flight checkpoint or extent
sealer makes it yield. A complete acknowledged suffix may be drained through
the unchanged native protocol only with exact accounting of at most 256
transitions and 1 MiB; unknown or larger work yields before materialization.
Complete current qualification and every hold are rechecked before deletion.
Pin capture also waits for extent sealing by yielding; it never joins that
worker while holding the writer gate.
All existing source and derived-reader holds remain binding. A later independent
durable checkpoint permits removal of at most 256 receipts. Reclamation and
prefix-refresh receipts cannot manufacture independent progress.

At an already drained boundary, admission retires the empty internal composite
publication cache using the existing operation. Captured readers retain their
pins. No journal flush is added. Receipt removal, logical command pruning, and
physical file retirement remain separate transitions, with receipt gate time
included in the existing maintenance pacing. Initial capture starts validation
only; the owner rests before requesting a subsequent source turn. The closed
shutdown diagnostics separate queue waiting from full actual source residence
and report only numeric admission, expiry, cancellation and drain-yield counts.
Admission changes scheduling, not deletion authority or any durable boundary;
required-host verification remains outstanding.

Checks run so far: the 24-test source-control suite, page/exact-end proof test,
stale-hold proof refusal, four process-crash cuts with repeated reopen, numeric
diagnostic test, and production-lane extent integration test pass. The latter
drives actual checkpoints, proves captured readers prevent physical retirement,
and proves retirement after release while retaining idempotent outcomes and the
latest command. The first scoped run exposed an extent-sealing race in that
integration test. Admission now yields while sealing is unfinished, and the
fixture explicitly joins the actual sealer. The eight-test online regression
subset and corrected integration test pass. The full rerun passed 1,644 of
1,647 tests. The remaining failures exposed an outdated gate-time assertion,
a catalog/storage architecture-boundary violation in the next implementation
slice, and a vector-health test's assumption about background worker readiness.
The assertion now separates receipt time; storage retains only private completion
flags rather than catalog proof objects; the vector test explicitly observes
projection readiness before asserting aggregate vector health. Individual
regressions pass. The next scoped run passes all 1,655 storage/server tests and
all seven acceptance steps, including Clippy, handbook checks, file-size guards,
and panic allowances. Full repository CI also passes for the committed slice,
including workspace tests, Clippy, dependency checks, handbook and generated
references, four-language operator conformance, existing recovery gates, and
release source installation. The implementation commit is `dbe974bd`.
This is not a closure receipt.

The next slice rebuilds optional validated-prefix evidence through the production
maintenance lane. A sealed private candidate requires all three complete passes
on the scrub's original immutable pin. Planning runs off gate. Publication uses
the existing Immediate receipt owner, refuses changed history, unfinished sealing,
a journal suffix, queued writers, or cancellation, and never overwrites newer
evidence from a stale snapshot. Shutdown cancels at page boundaries and joins
the bounded-stack maintenance thread before writer close. The maintenance lane
yields between receipt-validation pages instead of imposing its idle recheck
interval on every page; writer occupancy still determines maintenance rest.
Seven private completion/publication tests cover exact ends, original-pin
preservation, cancellation, writer admission, stale-candidate refusal, and
one-time maintenance publication. Four process-kill edges preserve either the
original state or one complete receipted checkpoint across repeated reopen.
The composed extent test now also rebuilds the checkpoint after pruning,
restarts without a clean-close ceremony through bounded dirty validation,
and verifies the next durable command and retained original outcome.

Implementation choices: use 64-row scrub pages, a 384-KiB maintenance stack,
the existing sampled startup inventories, private completion flags at the
catalog/storage boundary, and the existing checkpoint encoding and Immediate
publication owner. Require unchanged complete history for this initial
candidate admission. Account pin capture and publication in maintenance pacing;
continue receipt pages between explicit yields when a pass remains incomplete.

Compatibility: durable record formats, command sequencing, authorization,
idempotency, resume anchors, and existing extent retirement machinery are
unchanged. Updated handbook pages: [configuration](../configuration.md) and
this milestone page, both already reachable from `SUMMARY.md`.
The [startup integrity](../operations/STARTUP-INTEGRITY.md) handbook page also
distinguishes a missing checkpoint from a completely validated replacement.

Hazards and follow-ups: validated-prefix refresh has initial production wiring;
recovery to a successful write after pruning remains to be proved. Busy-workload
progress, repeated
physical generations, and the campaign gates remain unqualified. A stale
candidate is discarded rather than allowed to authorize deletion. No measured
rehearsal or alpha qualification is claimed.

## WP-823 population recovery measurement

The explicit storage-fixture test
`population_65536_first_successful_write_after_online_pruning_meets_60_seconds`
seeds 65,536 distinct commands in atomic groups of 64, prunes through the online
lane and restarts without refreshing prefix evidence or certifying a clean
close. It measures the next successful durable command, then checks that the
original first command's idempotent outcome survives. The final group is
retained whole when the requested 16-sequence floor intersects it: 65,472
sequences are pruned, rather than the incorrect initial expectation of 65,520.

The release-profile run on the local development host measured readiness at
24,549,339 microseconds and the first successful write at 24,552,101 microseconds,
below the unchanged 60-second target. It passed all assertions. Run explicitly:

```bash
cargo test --release -p riffdb-storage-redb --all-features \
  --test storage_recovery_matrix \
  population_65536_first_successful_write_after_online_pruning_meets_60_seconds \
  -- --ignored --nocapture
```

This is a storage recovery measurement, not TicketDesk or host qualification.
The fixture marks its outbox events delivered while stopped; production delivery
is covered separately by WP-822. The local machine has 32 CPUs, rather than the
frozen 8-vCPU rehearsal shape. A debug-profile attempt was stopped after its
structural audit validation exceeded the target; it remains a failed attempt,
not release performance evidence. The separate rehearsal host must measure the
real public workload and all required fault paths with its frozen artifacts.

PR note: Package WP-823 (open); Tier guarantee; Behavior: explicit population-
sized recovery measurement, including deferred validation and the next durable
write. Checks: the release measurement, formatting, scoped Clippy and allowed
paths pass. Compatibility: no production behavior, formats or interfaces change.
Updated handbook page: this milestone handoff. Hazards/follow-ups: full CI,
the integrated retention process-kill fixture and the real rehearsal remain open.

The integrated process-kill fixture now offers at most 32,768 public commands
and checks at most 64 directory entries per 64-command batch for an actual
sealed checkpoint extent. Its former 40-command seed stayed below the sealing
threshold and correctly remained ineligible for pruning. No forced checkpoint,
offline prune or production threshold change is used. The complete real-daemon
recovery matrix passes in release mode, including online pruning, a genuine
process kill, retained original-outcome replay and resumed durable writes.
Both recovery scripts select the shipping release profile; their existing
readiness and RPC deadlines remain unchanged.

Release execution also identified a race in the uncertain-replay fixture: it
expected its first projection request to be Ready even while the derived
generation was Building. The fixture now observes typed Building/Rebuilding
responses until the exact required frontier arrives within its existing
20-second deadline; failure and invalid responses still refuse the test.
All six scoped static checks pass, including the handbook and named artifacts.
The debug readiness failure and the first release projection assertion remain
failed attempts. Full CI and qualification remain open.

D003 decisions: seed through bounded public commands until ordinary checkpoint
publication, run the real-process matrix in the shipping profile, and synchronize
projection checks through typed frontier observations rather than execution speed.
Updated handbook pages: this milestone handoff and contributing instructions.

## WP-823 closure, October 4

WP-823 is complete. Earlier progress sections retain the failure history and
are snapshots, rather than the current package status. The fifth full CI run
passes at implementation revision `1bf6920d2`, with four concurrent tests and
six build jobs. The full release recovery matrix separately passes all ten
cases, including genuine process kill after online pruning. The population
measurement reaches the first durable write in 24.55 seconds. All nine scoped
acceptance checks pass for this closure's documentation and governance changes.

PR note: Package WP-823; Tier guarantee; Behavior: composed bounded receipt
reclamation and complete retained-prefix refresh; Checks: full CI, shipping
recovery matrix, population recovery measurement, source hold and physical
extent tests; Compatibility: unchanged ordering, durability, formats,
idempotency, authorization and application interfaces. Updated handbook pages:
configuration, startup integrity and this milestone handoff. Hazards and
follow-ups: the storage fixture's stopped outbox preparation is not production
qualification; WP-825 and WP-826 must demonstrate the real application and
repeated physical generations on the frozen host. No 24-hour run has started.

## WP-824 closure, October 4

WP-824 is complete; its [implementation record](WP-824-IMPLEMENTATION.md)
contains the accounting model, semantic tests, failure history and closure note.
Full CI passes at the same revision recorded for WP-823. Current authority,
minimum retention and active pins remain binding during retirement. A retired
historical module remains callable through ordinary fresh-owner admission.
The aggregate budget refuses protected saturation before allocating uncharged
workspace. WP-825 can now implement the actual campaign against completed
runtime prerequisites.

## WP-825 preparation progress

WP-825 is open. The separate `TicketDeskLifecycle` example lineage preserves
existing TicketDesk generated identities and the alpha endurance workload.
It adds an expected-revision update without creating tickets or comments,
emits `TicketUpdated`, declares a counting projection, and generates all four
language bindings plus MCP artifacts. The compiler rejects removal of the
caller fence at both replacement-write spans. Generation checks exercise the
real compiler and exact lock, rather than inspecting command names alone.

Build the preparation client and run its public TLS smoke:

```bash
cargo build -p riffdb-server --bin riffdbd -p riffdb-cli --bin riffdb \
  -p riffdb-ticketdesk --bin lifecycle
./scripts/ticketdesk-lifecycle-smoke \
  --root /tmp/riffdb-lifecycle-preparation \
  --server target/debug/riffdbd --cli target/debug/riffdb \
  --worker target/debug/lifecycle
```

The smoke uses generated commands, named reads, event acknowledgements and a
live-query snapshot. The real local run passes replay, stale-fence refusal,
returned-state, partition and event checks, followed by production signal
shutdown. The first harness attempt incorrectly combined a legacy projection
flag with named configuration and was refused. A second attempt passed client
checks but failed shutdown because closing stdin does not stop the TLS daemon.
The corrected harness selects a named owned root and sends the production
termination signal. `scripts/check-ticketdesk-lifecycle-preparation` builds the
four binaries and runs this real smoke in full CI. Failed attempts remain failed; no runtime check is relaxed.

The evidence checker has an explicit `--short-rehearsal` mode for WP-825. The
frozen target remains 24 hours and ordinary validation still requires all 288
windows. Short mode accepts fewer windows but preserves every other check,
including six final fault-free hours, all seven faults, three physically
reclaimed generations, service levels, resource ceilings and artifact binding.
Its success message explicitly says the 24-hour duration remains unmet. All 24
hostile-evidence tests pass; their synthetic fixtures are not measurements.

PR note: Package WP-825 (open); Tier guarantee; Behavior: generated lifecycle
application, bounded public preparation, and the accepted short-rehearsal
exception; Checks: all five scoped acceptance steps, Clippy, all 32 generators,
24 hostile-evidence tests, two compiler semantic tests, and the real CI smoke
entrypoint pass; requirement coverage and ADR obligations pass. Compatibility: existing application closure, alpha gate and normal
24-hour verifier remain intact. Updated handbook pages: this milestone handoff
and TicketDesk acceptance. Hazards/follow-ups: sustained offered-load scheduling,
queried projection observations, TypeScript smoke, lifecycle faults and
independent campaign reconciliation still require implementation and a real
host run before WP-825 closes. No 24-hour run has started.

## WP-825 population probe and independent oracle

Population preparation now records actual public native projection observations
during offered load rather than assuming zero backlog. One sequential auxiliary
lane samples health, then queries the selected projection's available frontier
without an after-sequence wait. Private bounded samples bind the exact lineage,
projection ID and plan hash and report the nonnegative difference between the
earlier sampled application head and the observed applied frontier. This is
sampled sequence lag, not simultaneous head or eligible-history byte evidence.
Every offered-load window needs a completed observation; unavailable, missing,
inconsistent or regressing evidence refuses success. Independent final aggregate
reconciliation is still required. The 50 evidence/preparation tests include
positive lag, missing-window refusal, selected-plan substitution, numeric bounds,
failure propagation and barrier-controlled observer shutdown. Real host
measurement of this sampler remains pending; this change qualifies no campaign.

The release preparation smoke also passes on the inventoried eight-vCPU host.
This establishes the real generated command/read/event/live-query carriage
there; it is not a campaign or a physical-reclamation result.

The Rust client now has a separate offered-load `probe` and full-population
`reconcile` mode. It assigns deterministic original update identities across
all 65,536 tickets, with explicit expected revisions. The independent oracle
rejects stale, missing, substituted, unoffered and wrong-partition results;
final reconciliation reads every ticket through named queries. Its baseline
without validation failed both rejection tests before implementation.

The load lane offers 100 commands and 400 named reads in each one-second batch,
with at most 28 admitted requests and four reserved auxiliary/control slots.
Its queue has 32,768 fixed slots; latency histograms are bounded. No production
concurrency, writer, durability or retention setting is changed. Latency includes
queueing from the offered time. Measurements close at the observation deadline;
later completions are separately disclosed. Submission counts cover settled
load command/named-read submissions; SDK same-key outcome lookups are auxiliary
reconciliation, rather than another command submission. This preparation uses
one submission attempt per SDK call and fails on transport errors or unexpected
replay; fault-aware original-identity recovery remains campaign work.

Six tests cover oracle substitutions, the final partial population round,
protected saturation, cancellation release and latency overflow. They and
Clippy pass. The updated real TLS smoke also passes. A new preparation option,
`--population-probe-seconds 300`, seeds the full population, runs a five-minute
load probe and reconciles every row. It must pass on real observations before
any rates are claimed. No population load result is claimed by this change.

PR note: Package WP-825 (open); Tier guarantee; Behavior: bounded load
preparation and independent generated-query reconciliation; Compatibility:
unchanged existing application identities, interfaces and qualification gates;
Updated handbook pages: this milestone handoff; Checks: six semantic/schedule
cases, Clippy and the real TLS preparation; Hazards/follow-ups: run the host
population probe, then complete active observations, faults and the qualifying
short rehearsal. The 24-hour run remains unstarted.

## WP-825 retained TypeScript smoke and host load preparation

The inventoried durable-SSD host completed the fresh five-minute population
probe at source revision `50ade9e2`: all 30,000 updates and 120,000 named reads
completed within the window, with zero retries, p95 73.830 ms and p99 84.490 ms.
The peak load concurrency was 28, and no completions were added after the
window. All 65,536 ticket rows reconciled afterward. Latency overflow remained
visible (392 samples above 250 ms); it was included in the quantiles. This
result covers offered load and state reconciliation, without lifecycle faults,
active campaign observers or physical reclamation qualification.

The preparation gate now also exercises retained generated TypeScript bindings
through the native Rust driver over verified TLS. It checks a named read,
wrong-partition refusal, a stale expected-revision command and its exact replay,
unchanged state, and a live-query snapshot. This smoke creates no durable
consumer that could hold later campaign history. Its build uses the existing
pinned runtime dependencies and stages compiled output only in the private
preparation root. The integrated real preparation gate passes.

PR note: Package WP-825 (open); Tier guarantee; Behavior: retained TypeScript
semantic smoke and observed full-population load preparation; Checks: real
Rust/TypeScript TLS preparation and host population reconciliation; Compatibility:
no database or public-interface changes; Updated handbook pages: this handoff;
Hazards/follow-ups: active observers, all seven faults, resource accounting,
archive/restore reconciliation, a frozen campaign and its short rehearsal remain
required. These observations do not qualify WP-825. No 24-hour run has started.


## WP-825 active observation preparation

The public TLS preparation gate now registers a generated event consumer and
live query before any seed command, waiting for an explicit ready message.
Two auxiliary slots cover the serial consumer calls and live stream. Its
bounded organization-zero oracle stores at most 1,024 latest ticket events,
checks creation/update fields and contiguous revisions, distinguishes equal
redelivery from substitution, and rejects gaps and partition mistakes. It
acknowledges through generated methods and checks native provenance metadata,
consumer leases/retries, incarnation and monotonic consumer/live frontiers.
Unplanned resets and lifecycle interruptions currently fail this preparation;
reconnect, rotation and restore handling remain campaign work.

The preparation queries `TicketUpdates` through the public operator CLI. A
first-party compiler descriptor checks the exact generated contract bundle hash
and supplies the projection ID and plan hash. Reconciliation requires that
identity, the organization-zero group, expected independent count and a frontier
at least as new as the sampled committed head. Native canonical capability
permission ordering is preserved. No runtime or application interface changes.

The real Rust/TypeScript TLS preparation with both observers and the queried
projection passes. Eight Rust semantic/schedule tests, two hostile projection
oracle tests and Clippy pass. The no-validation event and projection baselines
failed their rejection assertions before implementation. They are intentionally
failing tests, not database daemon panics. Raw failed-attempt logs remain private.

PR note: Package WP-825 (open); Tier guarantee; Behavior: bounded public event,
live-query and projection preparation observations; Checks: eight Rust tests,
two projection oracle tests, Clippy, integrated real TLS preparation and scoped
acceptance; Compatibility: unchanged native semantics and qualification gates;
Updated handbook pages: this handoff; Hazards/follow-ups: repeat full-population
load with observers on the durable-SSD host, complete actual faults, freeze the
campaign, reconcile archives/restores and run the required short rehearsal.
The 24-hour run remains unstarted.


## WP-825 observer scheduling and exact-parent successor

The first full-population attempt with observers ended with a terminal live
response while its serialized acknowledgement loop left the live head far
behind the committed head. Its failure
and logs remain preserved; it is not a passing observation result. Live-stream
draining now has a separate task within the same two auxiliary slots. A
deterministic bounded-channel schedule holds consumer acknowledgement while
both live updates advance, then stops the reader explicitly. Another test
rejects incarnation drift and decreasing frontiers without publishing them.

The additive fixture is an exact-parent V2 closure. Its extra compiled
`LifecycleVersion` command leaves all parent entities and command identities
intact. The public TLS application-lock workflow generates all bindings against
the active V1 parent; the independent compiler test checks the retained identity
and exact generated V2 bundle hash. Retired V1 bindings remain refused after
activation; preparation binds the current V2 role. Rust and TypeScript public
V2 smoke operations pass, including a positive fenced update, original-key
replay, unchanged stale refusal, named state and partition reconciliation.
The one-ticket preparation stops the old observer before activation. Continuous
observer handover and queued offered work remain actual campaign requirements.

PR note: Package WP-825 (open); Tier guarantee; Behavior: independent live
reader and exact-parent V2 preparation; Checks: eleven Rust semantic/schedule
cases, generated V2 closure, real two-version Rust/TypeScript TLS preparation,
Clippy, all 33 generators, three compiler/schema checks, current-role denial of
the added command, and scoped acceptance; Compatibility: native version/policy refusal and
qualification gates preserved; Updated handbook pages: this handoff;
Hazards/follow-ups: repeat the corrected observer population probe on the host,
complete the fault controller and independent campaign reconciliation, freeze
artifacts and run the required short rehearsal. No 24-hour run has started.


## WP-825 seeding and live-watch admission

The revised host attempt also reached native `buffer_pressure` while seeding;
its independent reader did not remove that source-side notification backlog.
Both failed observer attempts remain failed. The service retains its bounded,
fail-closed watch behavior. Population preparation now registers the durable
consumer before seeding, then starts the live query against the completed seed
before any offered-load window begins. An explicit file gate and observed live
progress coordinate that transition; the initial view must contain exactly the
expected top fifty seeded tickets with correct partition, identities and state.
The one-ticket CI preparation continues to cover a watch active during writes.

The observer also supports a private consumer-pause marker that stops issuing
consumer calls while the independently drained live stream continues. This is
controller preparation, not a measured five-minute outage. All twelve Rust
semantic/schedule cases and the integrated two-version TLS preparation pass.
No buffer, scan, service-level or qualification limit is raised.

PR note: Package WP-825 (open); Tier guarantee; Behavior: explicit admission of
live observation after population seeding and private consumer-pause control;
Checks: twelve Rust tests, Clippy, real two-version preparation and scoped
acceptance; Compatibility: native bounded refusal preserved; Updated handbook
pages: this handoff; Hazards/follow-ups: verify the host population probe with
this gate, implement and measure actual faults and observer handover, then
complete the frozen short rehearsal. The 24-hour run remains unstarted.

## WP-825 bounded watch renewal and consumer-outage preparation

The third observed host preparation failed with native live-query
`SERVICE_UNAVAILABLE` about thirty seconds after watch admission. That timing
matches the daemon's existing thirty-second request deadline. The observer now renews every
twenty seconds through the generated public watch method and opaque cursor,
cancelling the old request before admitting its replacement. A renewed snapshot
must retain exact partition and ticket identities, canonical ordering, and a
monotonic frontier. Renewal counts are actual observations. Native terminal
refusals and unexpected cursor resets still fail; no service bound changes.
The integrated one-ticket preparation now requires a real successful renewal.

Population load is monitored while it runs; observer exit, invalid or oversized
output, and deadline failure stop the owned load child. Before load, the seed's
1,024 organization-zero creation events must be consumed and checked. A new
optional fifteen-minute preparation schedules one real five-minute consumer
pause, requires frozen consumer progress and advancing live observation, times
the subsequent backlog drain, and reconciles all 65,536 final tickets. Private
nanosecond timings remain measurements, not a synthetic campaign receipt.

The separate sink preparation uses
`--population-probe-seconds 900 --observe --sink-outage`. It denies access to
the owned private archive directory for five minutes. The production connector
checks custody before claiming delivery; the preparation requires actual
degraded delivery health, advancing public write and live-query frontiers, and
delivery-drained health within ten minutes after restoring the same archive
objects, with offered load continuing. Pinned read-only descriptors serve only
identity checks and cleanup; no archive engine is opened by the controller.
Missing, changed or unavailable observations refuse preparation. Unit checks
cover custody preservation, substitution, cleanup and observation refusal;
real sink-fault measurement remains pending and no campaign is qualified.

PR note: Package WP-825 (open); Tier guarantee; Behavior: counted public cursor
renewal, supervised population load, and consumer-outage preparation; Checks:
twelve Rust cases, six preparation oracle/child-ownership cases, Clippy,
integrated two-version TLS preparation and scoped acceptance; Compatibility:
native request, retention, cursor and authorization guarantees preserved;
Updated handbook pages: this handoff; Hazards/follow-ups: measure the revised
observed host preparation, complete all remaining faults and version handover,
then freeze and pass the short campaign. No 24-hour run has started.

## WP-825 consumer preparation measurements and load quiescence

The first fifteen-minute host attempt completed 90,000 commands and 360,000
reads with no retries or post-deadline completions, and reconciled every seeded
ticket. Native live observation stayed active through 45 renewals without a
terminal refusal. Its preparation gate failed: the window overlapping consumer
recovery had p95 102.681 ms. The final normal window passed with p95 93.743 ms
and p99 140.542 ms. This failed attempt remains private and unqualified.

Preparation now applies the existing campaign checker's latency exception only
to windows overlapping the measured outage and drain, requires a complete
normal window, and leaves every throughput, error, concurrency and final-state
check binding. It also starts the full five-minute pause clock when the
observer confirms quiescence, rather than when the marker is requested.
Independent hostile tests reject an inactive watch, moving consumer, invented
recovery count, excessive timing, and a fault exception extending into the
normal window.

The load driver adds private controller admission/release controls. Quiescence
blocks new admissions while offered work continues into the same bounded
queue. An exact generated release or protected credential can change only
after every admitted original operation settles; rollback and repeated
generations refuse. A deterministic schedule verifies that resumption between
waiter registration and waiting cannot lose its wakeup. No authoritative
runtime or application-facing interface changes.

PR note: Package WP-825 (open); Tier guarantee; Behavior: confirmed consumer
pause timing, measured fault-window selection, and quiescent generated release
admission; Checks: fifteen Rust cases, seven preparation cases, Clippy, real
two-version TLS preparation and scoped acceptance; Compatibility: offered rates,
queue/flight bounds, native ordering, authorization and 24-hour/72-hour gates
preserved; Updated handbook pages: this handoff; Hazards/follow-ups: repeat the
corrected observed host preparation, integrate actual deployment/observer
handover and remaining faults, then pass a frozen short campaign. No 24-hour
run has started.

## WP-825 measured consumer recovery and observer handover

The corrected fifteen-minute preparation passed on the inventoried 8-vCPU,
32-GiB durable-SSD host. It completed 90,000 commands and 360,000 reads with no
retries or post-deadline completions, reconciled all 65,536 tickets, and checked
3,072 consumer events without duplicates. The confirmed consumer pause lasted
300.050 seconds; its missing-event backlog drained in 13.089 seconds. The final
normal window measured p95 85.436 ms and p99 138.481 ms. This is preparation
with one fault and retained history, not a campaign or physical-reclamation gate.

The same observer now survives exact-parent V2 activation. It finishes and
acknowledges its current batch, cancels the watch, and publishes an atomic
quiescence receipt. Before activation the operator retires the V1 consumer.
After activation the V2 application credential registers the successor consumer
and negatively acknowledges its exact initial lease. The operator then seeks
that consumer to the recorded acknowledged checkpoint. Fresh status must match
the checkpoint and incarnation, with no leases, retries or dead letters. The
observer preserves its event oracle and resumes the generated V2 watch; the
integrated TLS preparation verifies the subsequent V2 event without duplicates.
Seek authority is confined to the operator credential. No application role,
native runtime boundary, durable format, request bound or ordering changes.

PR note: Package WP-825 (open); Tier guarantee; Behavior: quiescent consumer and
watch handover through exact generated releases and existing public maintenance
operations; Checks: seventeen Rust semantic/schedule cases, Clippy, real
two-version Rust/TypeScript TLS preparation and scoped acceptance;
Compatibility: existing authority, history, lease and checkpoint checks remain
binding; Updated handbook pages: this handoff; Hazards/follow-ups: exercise
handover under online retention, finish original-identity fault recovery and all
campaign actions, then freeze and pass the short rehearsal. The 24-hour run
remains unstarted.

## WP-825 original-input interruption recovery

The load worker now resolves interruptions with the original generated command
input and idempotency key. Recovery requires a private declared restart, process
kill or restore interval and a native retry/uncertainty disposition. Permission,
input, protocol and response failures remain immediate refusals. One operation
keeps its original release and admission slot throughout recovery; its deadline
is sixty seconds with at most 240 generated submissions. Clearing a fault marker
cannot abandon an already interrupted identity or extend its deadline.

The integrated TLS preparation now also stops and restarts the real daemon,
and separately kills and reopens it. Each check first connects the generated
client, proves an actual failed submission while the daemon is absent, resumes
that same client, and requires a successful write, exact original-key replay
and named-state reconciliation within sixty seconds of the interruption.
These are small preparation databases with retained history; they do not prove
recovery after campaign pruning or a committed command losing its reply.

Load observations count settled generated submissions and their retries. The
native SDK can additionally look up an uncertain outcome; the submission count
is not represented as an exact wire-RPC count. Campaign accounting must preserve
that distinction. The only added dependency is the existing first-party error
crate, so the worker uses its canonical recovery guidance directly.

PR note: Package WP-825 (open); Tier guarantee; Behavior: bounded original-input
interruption recovery and actual daemon restart/kill preparation; Checks:
Rust semantic/budget cases, Clippy, integrated Rust/TypeScript handover plus
two interruption preparations, and scoped acceptance; Compatibility: generated
input, release, idempotency and authority boundaries preserved; Updated handbook
pages: this handoff; Hazards/follow-ups: complete independent command/archive
witnesses, campaign fault orchestration and pruning measurements before the
frozen short rehearsal. No 24-hour run has started.

## WP-825 acknowledgement witnesses and state reconciliation

Private seed and load witnesses now record each acknowledged ticket command's
ordinal, commit sequence, exact contract version and history incarnation. The
files have fixed, bounded slots and private permissions; records can arrive out
of order, but a second acknowledgement cannot overwrite a slot. Completion
requires every expected slot and synchronizes the evidence file. Missing,
truncated, malformed, oversized and linked inputs refuse reconciliation.
These files are independent test evidence, never authoritative database storage.

An immutable witness snapshot reconstructs each ticket's acknowledged revision,
including holes at a quiescent backup cut. A later revision without its earlier
ticket revision refuses. Population preparation now compares every named row
against these witnesses rather than relying only on an offered-command counter.
Private release controls also carry the expected history incarnation; an
observer resumption must confirm it through native consumer status.

The small interruption preparations passed on the inventoried host as well:
graceful restart took 0.375 seconds and process kill took 0.323 seconds through
successful write, exact replay and named-state checks. Each had one retried
generated submission. These results do not establish recovery after pruning.

PR note: Package WP-825 (open); Tier guarantee; Behavior: independent bounded
acknowledgement witnesses and witness-based named-state reconciliation; Checks:
Rust witness identity, corruption, gap and out-of-order cases, Clippy, real TLS
preparations and scoped acceptance; Compatibility: authoritative formats and
application interfaces unchanged; Updated handbook pages: this handoff;
Hazards/follow-ups: validate archive payloads against these witnesses, complete
campaign actions and actual physical-retention measurements, then pass the
frozen short rehearsal. The 24-hour run remains unstarted.

## WP-825 independent offline archive reconciliation

The private Rust operator checker now opens the stopped database only for its
dormant identity probe, then uses the existing exclusive durable-archive owner.
It validates every archive page through the native envelope/hash checker,
matches each witnessed database/incarnation/event identity, and independently
reconstructs complete TicketCreated and TicketUpdated payloads from the ordinal
and exact compiler-owned schema. Declared policy anchors must match exactly;
these workload declarations use the accepted unanchored event shape, whose
absence is also checked. Writer-version evidence comes from command replies in
the witness; unanchored archive records do not independently encode that version.
All records must be accounted for, with bounded steps and no payload diagnostics.

The integrated preparation checks two real archived events after production
shutdown. Hostile witness substitutions prove that valid native archive hashes
and counters cannot excuse a different event or history incarnation. Population
preparation also performs this offline reconciliation after stopping its daemon.
The new dependencies are existing first-party compiler, semantic storage, archive
and type owners; the operator checker adds no engine decoder or runtime mutation.

The first witness-enabled five-minute host preparation completed all 30,000
commands and 120,000 reads and reconciled all 65,536 tickets. Its p95 100.550 ms
missed the normal-window limit; p99 was 139.531 ms. It remains failed. Recovery
controls are now consulted only after a native failure or returned replay,
rather than by every successful operation. A repeat measurement is still needed;
no latency target or workload rate has changed.

PR note: Package WP-825 (open); Tier guarantee; Behavior: offline native archive
validation against independent acknowledgements and actual semantic substitution
tests; Checks: twenty-three Rust cases, Clippy, four real TLS preparations,
archive substitution cases and scoped acceptance; Compatibility: native archive
formats, authority and qualification gates preserved; Updated handbook pages:
this handoff; Hazards/follow-ups: repeat the host preparation, measure online
retention and pruning recovery, finish all campaign actions and conformance,
then pass the frozen short rehearsal. No 24-hour run has started.

## WP-825 delivery admission and physical-retention observation

Offline inspection of the failed host population database found a missing
archived event. That additional failure is preserved; the database is not
qualified by its completed commands or reconciled entity state. Population
preparation now observes the public outbox component's explicit healthy status
before starting load and before stopping the daemon. The initial seed drain and
post-load drain have separate bounded timing records. These waits occur with
offered writes stopped and cannot prove the campaign's outage-drain requirement
at continuing offered load. The revised host measurement remains pending.

The optional `--online-retention` population preparation selects the existing
32,768-sequence retention target and 256-sequence maintenance budget. A private
read-only observer inventories actual sealed file identities once a second and
records their disappearance. Reappearing generations, changed identities,
links, excess entries, or disappearance exceeding the production deletion/reuse
counters refuse evidence. Shutdown counters also retain reference and reader
holds. The observer's tests use explicitly synthetic files, not reclamation
claims. A short preparation reports actual counts without claiming the three
physically reclaimed generations required by the complete rehearsal.

PR note: Package WP-825 (open); Tier guarantee; Behavior: explicit public archive
drain observation and physical file/counter inventory under existing online
retention; Checks: eleven preparation cases, thirty-four evidence/checker cases,
integrated TLS preparation and scoped acceptance; Compatibility: no runtime,
retention, archive or qualification guarantee changes; Updated handbook pages:
this handoff; Hazards/follow-ups: measure the revised host run and online
retention, finish campaign faults and current-language conformance, then pass
the frozen short rehearsal. The 24-hour run remains unstarted.

## WP-825 explicit workload transport accounting

The private measured worker now submits updates through the existing native
`RiffDbClient::execute_generated` API with one attempt. Compiler-generated V1/V2
inputs and outcome decoding remain authoritative. Its retained-input recovery
loop resubmits the original command identity; it does not invoke the SDK
facade's automatic outcome lookup. Reads retain the generated named-query
facade, which submits one query RPC. Measured update/read attempts therefore
include every explicit retry while the original in-flight reservation remains
held. Auxiliary consumer, watch and operator calls remain separate from offered
load and must be counted separately by the campaign controller.

The worker exposes `transport_attempts` alongside the earlier submission field.
Historical preparations keep their original observations and accounting limits;
the new field cannot retroactively qualify them. This change does not alter
server recovery semantics, authorization, native SDK behavior or public
application bindings.

PR note: Package WP-825 (open); Tier guarantee; Behavior: private workload
one-attempt native generated submission and explicit original-input recovery;
Checks: workload semantic tests, Clippy, public TLS preparations and scoped
acceptance; Compatibility: existing protocols and generated bindings preserved;
Updated handbook pages: this handoff; Hazards/follow-ups: complete auxiliary
accounting, all lifecycle actions and language conformance, then qualify the
frozen short rehearsal before the 24-hour run.

## WP-825 native blockers before further harness development

Harness expansion is paused for native diagnosis and repair. The required host
preparation with online retention and a consumer outage failed with live-query
`BufferPressure`. It logically pruned 115,460 sequences but released zero source
receipts and zero physical extents. A deterministic storage regression now
reproduces zero receipt progress across 24 interleaved durable writer turns;
its positive control releases 256 receipts with the same owner and data after
an idle validation pass and a later independent durable checkpoint.

The real capability-rotation preparation initially failed because revoked-token
command admission emits a details-free permission denial that the native SDK
correctly rejected as a protocol violation. The native fix supplies the
existing structured kernel authorization refusal at the server boundary and
preserves symbolic `CapabilityRevoked` responses. Client protocol validation
remains strict. All 114 API gRPC unit tests and scoped Clippy pass. A fresh
actual TLS rotation preparation now proves old-credential refusal and exact
same-principal replacement replay, state and archive reconciliation. This does
not establish rotation under continuing campaign load.

The promoted backup recovery timeout from full CI remains unexplained. Its
isolated rerun and a complete 25-test replication rerun pass; neither erases
the original failure or establishes full CI. The failing reclamation regression
is intentionally unresolved. The maintainer approved necessary native repairs;
the proposed source-proof continuation amendment, ADR-0268,
still requires exact-text acceptance before implementation.
See [the concrete repair scope and findings](WP-825-LIVE-OBSERVATIONS.md).
No failed preparation qualifies the milestone, and no 24-hour run has started.

PR note: Package WP-825 (open); Tier guarantee; Behavior: repair native kernel
revocation error carriage while preserving symbolic refusal and strict SDK
decoding; Checks: 114 API gRPC tests, scoped Clippy, actual TLS rotation
preparation and handbook checks pass; scoped storage acceptance still fails only
the new reclamation regression (1,141 other tests pass), and all 25 replication
tests pass on rerun; Compatibility: existing error encodings, authorization
decisions, durable formats and qualification gates preserved; Updated handbook
pages: application errors, known limitations, native findings and this handoff;
Hazards/follow-ups: accept the reclamation amendment, resolve native starvation
and explain live-query and backup readiness behavior before resuming campaign
implementation. Full CI and the complete short rehearsal remain pending.

The next native candidate makes authoritative commit readers use the existing
graph-owned command-admission plan cache. A real catalog regression measured
17 full bundle reads for 17 mappings of one commit; reuse needs one. Every
lookup still checks the current durable active pointer, and cold or changed
identity still requires full native validation. Controlled concurrent first
reads and deployment races, historical outcomes, forged plans and missing
current pointers are covered. All 25 read-adapter tests and scoped server
Clippy pass; the complete server unit suite passes 556 tests with one existing
skip. Notification buffers, workload rates and watch terminal behavior
are unchanged. The host overflow remains an unresolved failure until actual
load evidence explains it.

PR note: Package WP-825 (open); Tier guarantee; Behavior: share the already
bounded exact-plan owner with authoritative commit readers, preserving durable
pointer qualification and full cold validation; Checks: red/green bundle-read
regression, deterministic concurrency/deployment schedules, 25 adapter tests
and scoped Clippy, 556 server unit tests, handbook checks; Compatibility: no
public interface, error class, durable
format, authority or capacity change; Updated handbook pages: native findings
and this handoff; Hazards/follow-ups: actual required-host load verification,
native reclamation repair, full CI and complete short rehearsal remain required.

The focused required-host preparation at `a69d664c` completed 90,000 commands
and 360,000 reads, the actual 300.049-second consumer pause and its 10.027-second
drain, with live progress through 45 renewals. It still **failed**: the final
fault-free five-minute window had 1,676 requests above 250 ms, so p99 missed
the gate despite p95 of 83.413 ms. The bounded profiling samples ended before
that window. Source receipts and physical extents again showed zero reclamation.
No final archive reconciliation or qualification follows a failed latency gate.
The complete findings also identify a separate full-population prefix-refresh
scheduling concern. The pending source-proof amendment does not waive that
boundary or any requirement. No 24-hour run has started.

PR note: Package WP-825 (open); Tier guarantee; Behavior: record actual required-
host preparation failure and the additional native scheduling concern; Checks:
all 450,000 offered requests completed, exact consumer outage/drain recorded,
live progression observed, unchanged normal-window latency gate refuses the
run, production shutdown reports zero reclamation; Compatibility: unchanged
workload, concurrency, latency, source authority and recovery requirements;
Updated handbook pages: this handoff and native findings; Hazards/follow-ups:
accept ADR-0268 exactly, repair reclamation, diagnose latency and prefix refresh,
explain the intermittent recovery failure, then complete all rehearsal gates.


ADR-0268 was accepted exactly on 2026-10-04 in standalone commit `313f8e77`.
Native continuation now passes the unchanged 600-command, 24-writer-turn
starvation regression. It also repairs the drained journal source allocator
after reclamation and live validated-prefix publication. Completed certificates
retain checked native state without an obsolete reader pin between turns.
The [native findings](WP-825-LIVE-OBSERVATIONS.md) describe mutation coverage,
corruption and recovery checks, streamed-row measurements and bounded numeric
diagnostics. Required-host load, prefix-refresh scheduling, latency, the
intermittent recovery failure, full CI and the complete short rehearsal remain
open. No 24-hour qualification has started.

The failed required-host database exposed two further native qualification
defects: canonical audit keys were misclassified, and command pruning's retained
Service audit materialization invalidated source proof. The native repair uses
actual preimages and the existing key/record decoders. A new 600-command,
24-turn composition regression now reclaims receipts with a write, checkpoint
and logical prune on every turn; malformed or mismatched evidence still refuses
qualification. Required-host physical progress and latency remain unverified.

PR note: Package WP-825 (open); Tier guarantee; Behavior: qualify canonical
audit appends and checked command-audit representation changes during native
journal admission and checkpoint materialization; Checks: deterministic native
composition, full-source oracle and hostile evidence tests, scoped Clippy;
Compatibility: unchanged durable formats, audit semantics, source-control
fences, ordering and public interfaces; Updated handbook pages: this handoff,
native findings and known limitations; Hazards/follow-ups: required-host
continuing-load verification, full acceptance, intermittent recovery failures
and complete short rehearsal remain required.

PR note: Package WP-825 (open); Tier guarantee; Behavior: bounded native source
continuation under accepted ADR-0268, exact deletion preimages, completed-pin
release and drained source-allocation publication repair; Checks: targeted
native progress, corruption, hold, population, pin and process-exit tests,
registered/fence/promotion oracles and scoped Clippy; the full storage suite
ran 1,151 tests (1,148 passed, three architecture layout assertions failed).
All 59 architecture tests pass after updating those assertions to the sealed
footprint commit owner; full package acceptance/CI are not yet established; Compatibility:
unchanged durable formats, ordering, authorization, application semantics and
validated-prefix proof requirements; Updated handbook pages: this handoff,
native findings, TicketDesk acceptance and known limitations;
Hazards/follow-ups: source acknowledgement changes conservatively invalidate;
required-host physical progress and latency, refresh scheduling, recovery
investigation and the complete short rehearsal remain required.

The real local preparation also passes V1/V2 TLS checks through Rust,
TypeScript, Go and Python, plus graceful restart, kill, archive/retention and
capability rotation preparations. These checks retain exact replay, revision,
organization and bounded snapshot assertions. They do not establish the
hash-bound campaign, continuous-load fault coverage or required-host gates.

The subsequent scoped acceptance observed an unsuccessful daemon shutdown in
the repeated-kill replication test. Its isolated repeat passes; the refusal
remains unexplained. Test-only, bounded closed failure-code evidence now makes
the next occurrence diagnosable without retaining raw child diagnostics.
The [native findings](WP-825-LIVE-OBSERVATIONS.md) record that limitation.

PR note: Package WP-825 (open); Tier internal within the guarantee package;
Behavior: closed fixture failure-code evidence after unsuccessful daemon exit;
Checks: source-redaction unit test, real invalid-shutdown process test and
isolated repeated-kill replication test pass; Compatibility: no production
interface, format, deadline or successful lifecycle check changes;
Documentation: this handoff and native findings; Hazards/follow-ups: the original
shutdown refusal and promoted-backup timeout still require diagnosis, and the
final CI receipt must bind a clean final revision.

The unprofiled required-host probe of `2fb963b7` still refuses normal-window p99.
It completed 450,000 requests with zero retries, but production removed zero
receipts or extents. New counters reveal 288 source-proof invalidations and no
continued suffix work. The append-only regression does not cover that native
composition; repair its invalidation/proof handling before campaign expansion.
The [native findings](WP-825-LIVE-OBSERVATIONS.md) preserve the failed run.

Optional prefix refresh now discards a changed root at history, structural and
catalog page boundaries while preserving complete exact-root publication.
This is an implementation choice within the existing full-proof requirement,
not permission to publish partial evidence. Actual source progress and latency,
stable refresh scheduling, intermittent recovery and the complete rehearsal
remain open. No 24-hour qualification has started.

PR note: Package WP-825 (open); Tier guarantee; Behavior: exact-root early
discard of stale optional prefix work, closed fixture diagnostics and bounded
read-only namespace forensics; Checks: 11 native prefix/publication tests and
six static scoped acceptance steps pass; the previous full scoped acceptance
had three replication process failures; Compatibility: unchanged full prefix
proof, durable encodings, application semantics, deadlines and fail-closed
backend refusals; Documentation: this handoff and native findings;
Hazards/follow-ups: actual composition still invalidates every source candidate,
normal-window p99 fails, stable refresh paging and recovery diagnosis remain
open, and clean final CI plus the complete short rehearsal are still required.

## WP-825 fair-admission host diagnostic

Native revision `1841f1f1` passes full CI and the release source installation
check. Its unchanged 900-second consumer-outage preparation on the required
8-vCPU, 32-GiB durable-SSD host still fails normal-window latency. All 90,000
commands and 360,000 reads complete without retries, and full population state
reconciliation passes. The final five-minute window has 11,565 requests above
250 ms, so both p95 and p99 fail. The measured consumer pause starts at 5.267
seconds, lasts 300.048 seconds and drains in 5.715 seconds; only the first two
windows overlap that outage and recovery. The failed preparation is preserved.

Whole-preparation native counters report 61,884 source receipts removed, one
extent recycled and none deleted. Source admission grants 570 of 572 queued
turns, with two expirations; continuation streams 413,586 rows after 178 initial
rows. Its last planning sample has resume 61,629, checkpoint ceiling 364,681
and tail 372,644, with no source holds observed at that barrier. These are
planning positions and cumulative counters, not eligible bytes, per-pass
durations or proof of physical reclamation for the retained range.

The production scheduling regression separately demonstrates that actual
source deletion still receives a one-second idle recheck when logical retention
is held or at its target. Refining that policy requires exact review under
ADR-0271; it has not been implemented. The new sink-outage and queried-projection
preparations pass 50 semantic tests and five integrated scoped checks. Their
separate host diagnostic remains unqualified. Native performance, measured
resource classification, complete fault coverage and the qualifying short
rehearsal remain open. No 24-hour run has started.

PR note: Package WP-825 (open); Tier internal; Behavior: record measured failed
host preparation and its limits; Checks: 50 evidence/preparation tests and five
integrated scoped checks pass; Compatibility: no native behavior or guarantee
changes; Updated handbook: this handoff; Hazards/follow-ups: exact pacing review,
normal-window performance repair and complete milestone evidence remain pending.

## WP-825 source-progress rechecks

ADR-0272 is accepted. Source steps that actually commit receipt deletion use a
progress recheck even when the separate logical watermark is held or at its
target. They still rest for the complete actual writer residence using the
unchanged factor and cap. Zero removal keeps the existing idle backoff; floor
plans, validation counters and granted requests cannot select progress. The
production scheduling regression passes after failing on the preceding policy.
The deterministic rest test covers complete phase accounting, exclusion of
queue waits, idle backoff and cancellation before every rest duration. Public
health labels, checkpoint waits, safety checks and deletion bounds are unchanged.
All nine scoped checks pass, including 632 server tests with eight existing
skips. Full CI passes at native revision `6a6128fb`, including conformance and
the final source-installation check. The first CI attempt stopped because its
environment omitted the pinned TypeScript compiler from `PATH`; that attempt
is preserved. Repeating the complete battery with the repository's locked
TypeScript 7.0.2 compiler passes on unchanged source. A fresh required-host
900-second consumer-outage preparation completed with the exact verified
native binaries. All 90,000 commands and 360,000 reads completed without retries;
full state and archive reconciliation passed. The final normal window measured
p95 56,425 us and p99 77,406 us. Consumer recovery took 8.674 seconds after its
300-second pause. These results cannot support closure: native retention entered
its terminal `refused` state after 54 recorded attempts, removing no source
receipts, advancing no logical retention steps and retiring no physical extents.
Latency with that stopped lane does not prove sustainable lifecycle performance.

PR note: Package WP-825 (open); Tier guarantee; Behavior: actual committed source
deletion receives progress rechecks with unchanged gate rest; Checks: native
scheduling, rest and worker lifecycle tests, nine scoped checks and full CI at
`6a6128fb`; Compatibility: unchanged public controls, formats, ordering, holds,
checkpoint evidence and durability; Updated handbook: this handoff;
Hazards/follow-ups: diagnose the native terminal refusal, then establish
required-host latency with actual reclamation and all remaining campaign gates.
This repair does not qualify a rehearsal or start the 24-hour run.

## WP-825 native health wire correction

The separate sink/projection diagnostic stopped before fault injection because
the harness incorrectly expected an integer health sequence. Native CLI health
correctly returns a canonical decimal string. The real projection query was
Ready at sequence 65,731 after health sampled 65,744, proving an observed lag
of 13 sequences; the harness failed to record it. Both the projection sampler
and sink oracle now validate the native decimal representation before numeric
comparison. All 51 evidence/preparation tests pass, including rejection of
JSON numbers, malformed decimals and overflow. The failed preparation remains
preserved. This correction changes no native interface, query semantics or
milestone gate. The repeated consumer preparation collected queried projection
samples successfully. The separate sink preparation collected 48 samples before
the native retention thread's stack overflow described below.

## WP-825 preparation memory observations

Population preparations now write private `memory-samples.jsonl` observations
of the controller, server, Rust load worker and Rust observer every five seconds.
Every load window needs an actual sample. The collector reads bounded Linux
process status, checks process start-time identity before and after collection,
and refuses missing fields, exit, zombie state, substitution, duplicate fields,
overflow and reused process identifiers. It reports counts without process
identifiers, process names or filesystem paths. Collection uses the controller
thread and adds no application request slot.

The `allocator_bytes` diagnostic is explicitly a **whole-process virtual address
space upper bound**, including executable/file mappings, thread stacks and
reserved or free address space. It is not allocator live bytes. `VmData` is not
used as a substitute for allocator accounting. The record names its basis and
sets `qualifies_resource_model` to false. These four-process samples exclude
transient CLI children and unobserved peaks between samples; they cannot set
the campaign's `resource_model_verified` check. Qualifying accounting still
requires the complete process inventory, a frozen disclosed resource model and
native eligible-history byte observations. No per-command allowance may excuse
process-memory or reclaimable-history growth.

All 61 checker/preparation tests pass, including real owned child collection,
deterministic replacement during a sample, reused process identity, missing
window coverage and preservation of private failed observations. The required-host
sink repetition collected 48 observations before a native stack overflow. Complete
campaign resource accounting and a successful repetition remain pending.

PR note: Package WP-825 (open); Tier internal; Behavior: bounded private
preparation memory diagnostics with explicit incomplete scope and conservative
basis; Checks: 61 evidence/preparation tests pass, including a signal-scheduled
real child collection test; Compatibility: unchanged native/public interfaces,
request limit and campaign gates; Documentation: this handoff and the lifecycle
example; Hazards/follow-ups: transient process accounting, observed peaks,
native eligible-history bytes, host repetition and the complete short rehearsal
remain required before qualification.

## WP-825 native maintenance refusal acceptance gap

The consumer preparation returned success despite the native shutdown census
reporting `online_retention=refused`. Its command, state and archive checks were
valid, but omitted this independent maintenance outcome. Online-retention
preparations now require the native lane's normal `cancelled` owner-shutdown
state. Missing, malformed, repeated inconsistently or refused terminal evidence
fails preparation. Actual extent observations are retained before that refusal;
zero removed files never becomes proof of reclamation.

The new regression fails on the previous collector for all seven hostile
terminal inputs and passes after the correction. All 62 checker/preparation
tests and all five scoped acceptance checks pass. The separate sink preparation
used its frozen preceding controller; its native fatal failure is recorded below.

PR note: Package WP-825 (open); Tier internal; Behavior: refuse online-retention
preparation success when native maintenance stopped or its terminal evidence is
missing or malformed; Checks: 62 checker/preparation tests and five scoped
acceptance checks; Compatibility:
unchanged native behavior, public interfaces and qualification thresholds;
Updated handbook: this handoff and lifecycle example; Hazards/follow-ups:
native refusal diagnosis and repair, measured reclamation and complete short
rehearsal remain required. No 24-hour run has started.

## WP-825 closed native refusal diagnostics

Terminal maintenance refusal now emits one private, bounded observation naming
only numeric codes for the failing phase and existing closed storage error kind.
Phase 1 is `step`; phase 2 is `prefix_refresh`. Kind codes 1–8 mean unavailable,
unknown commit status, corrupt data, incompatible format, limit exceeded,
invariant violation, sequence exhausted and pruned history, respectively.
The previous handler discarded this cause before recording
its generic incident. The observation includes no source error, incident
identity, filesystem path, payload or caller text. Normal shutdown cancellation
remains separate; native refusal still stops the lane without retrying past it.
No public health field, RPC, durable format or maintenance policy changes.

The regression fails on the previous generic diagnostic and checks every
storage error kind in both phases. All six focused retention-worker tests pass,
including actual native refusal, terminal-state preservation, cancellation and
unchanged source pacing. The broader test run passes all 633 server tests across
17 binaries, with eight existing skips. Its initial acceptance fails formatting;
after correcting the formatting and narrowing the diagnostic to numeric codes,
all six focused worker tests and six scoped non-test checks pass. Required-host
reproduction with these diagnostics is
still pending; the exact native refusal cause has not yet been measured.

PR note: Package WP-825 (open); Tier guarantee; Behavior: retain closed private
phase/kind evidence at the existing terminal refusal; Checks: six focused native
worker tests, 633 broader server tests, 62 checker/preparation tests and six scoped
non-test checks pass; Compatibility: unchanged
public interfaces, native refusal, durable formats, ordering and pacing;
Updated handbook: this handoff; Hazards/follow-ups: exact native reproduction
and repair, full CI and complete lifecycle qualification.

## WP-825 sink diagnostic native stack overflow

The separate `15f133c7` controller / native `6a6128fb` sink-outage diagnostic
failed around 241 seconds into offered load, during the archive pause. The
server's native `riffdb-retention` thread overflowed its stack and aborted the
daemon. The next sampled health call produced no response before its deadline;
observer and daemon cleanup also failed. No complete load window or final state,
archive or extent reconciliation was produced. All failed artifacts are retained.

There were 48 successful projection and four-process memory observations before
the abort. The last memory observation recorded server RSS 689,913,856 bytes
and virtual address space 4,540,047,360 bytes; these sampled values do not measure
the failing thread's stack or establish complete resource accounting. An earlier
native source floor plan observed resume 89,826 and prior checkpoint 148,863;
these positions are not byte measurements or a substitute for the missing final
removal census. The fatal native stack path needs diagnosis and repair. Increasing
sampling deadlines or hiding the abort cannot qualify this milestone.

## WP-825 iterative publication-lineage retirement

A process-isolated storage regression reproduces a native stack overflow when
the default destructor retires the supported 65,536-node publication lineage
on a 384-KiB maintenance stack. The lineage fits the existing allocation bound.
This demonstrates a database defect independently of the workload controller.
It does not yet establish the call path of the required-host abort.

Retirement now detaches uniquely owned predecessor links iteratively, using
`Arc::into_inner` to preserve ownership when snapshot owners retire concurrently.
Shared predecessor nodes remain intact until their final owner releases them.
The regression checks surviving snapshot contents and ancestry, final release
of every node, and concurrent release from distinct successors sharing a chain.
Stack sizes, overlay limits, qualification checks, durable encodings, writer
ordering and maintenance pacing remain unchanged.

The initial regression aborts before the repair; all 325 storage API unit tests
pass afterward. All eight scoped acceptance checks pass, including 555 tests
with all features enabled. The strengthened retirement regression also passes
in the release profile. Required-host testing and full CI on the final source
remain pending. A local debugger workload did not reproduce the host abort;
it ended with incomplete sink recovery and supplies no qualifying evidence.
No 24-hour run has started.

PR note: Package WP-825 (open); Tier guarantee; Behavior: retire supported
publication ancestry without recursive stack growth while preserving shared
snapshot pins; Checks: failing process-isolated reproduction, 325 unit tests,
555 all-feature scoped tests, release-profile regression and eight scoped checks;
Compatibility: unchanged public interfaces, durable bytes, ordering and limits;
Updated handbook: this handoff and live observations; Hazards/follow-ups: confirm
the host crash call path, diagnose terminal refusal, run final-source full CI
and repeat the unchanged required-host workload.

## WP-825 joined retention shutdown census

The daemon's shutdown-trigger observation is captured before the retention
thread joins. Previously it remained the final census even when that thread
later cancelled normally. A healthy lane could therefore report its live idle
or fenced state, causing the terminal-state collector to reject it. This does
not invalidate the earlier measured refusal: refused owners retain that state.

The retention owner now replaces the live observation after joining its thread
and releasing source-validation pins. It records the actual final state and
counters; it never substitutes cancellation for refusal. The real native-worker
regression fails on the old census and passes after the repair, checking both
healthy cancellation and preserved terminal refusal. All six focused worker
tests pass with all features enabled. Scoped acceptance passes all 13 checks and
all 633 server tests, with eight existing skips, using an owned temporary root.
The initial broad run fails six tests while concurrent runners share temporary
storage across separate PID namespaces. Observed stale-directory sweeps can
remove another runner's live fixtures. That failed run and diagnostic traces
are retained; the isolated rerun keeps every assertion and deadline unchanged.
Final-source full CI and host testing remain pending; the public health labels
and census field registry are unchanged.

PR note: Package WP-825 (open); Tier guarantee; Behavior: record retention
shutdown evidence after owner join; Checks: baseline regression fails, six
focused all-feature worker tests and all 633 scoped server tests pass, with
13 scoped checks; Compatibility: unchanged storage, public
interfaces, ordering and maintenance policy; Updated handbook: this handoff and
live observations; Hazards/follow-ups: final-source acceptance, CI, native refusal
diagnosis and unchanged host workload before lifecycle qualification.

## WP-825 maintenance worker drain

Ordinary shutdown explicitly joins the durable-local dispatcher, online
retention owner and background scrub before draining the coordinator. The
maintenance path omitted those joins and deferred them to field destruction
after its drain boundary and graceful close. A real public backup regression,
with a configured local archive and online retention, reaches `DrainComplete`
with background task handles still owned. The dedicated crash fixture refuses
that boundary rather than reporting the expected crash. The failed run remains
failed.

Maintenance now explicitly joins those same owners before coordinator drain.
The process fixture checks actual task ownership at the crash boundary; it
does not inspect source text or change the production crash controller. Its
recovery arm also checks the original command outcome and provenance, a first
successful subsequent write, and that write's exact idempotent replay. The
preparation gate runs this arm before TicketDesk lifecycle smokes. The release
regression passes. Scoped acceptance passes all 15 checks and all 798 tests,
with eight existing skips, using owned temporary storage. Full CI and the
unchanged required-host diagnostic on the final source remain pending.

The earlier full CI on the joined-census source fails one archive-publication
test at sealing with `Unavailable`. The unchanged exact test and the complete
parallel storage suite subsequently pass (876 tests, one existing skip). Those
passes do not explain the original failure or replace full CI. A separate older
CI stops at the redirected TypeScript build because its isolated checkout lacks
the pinned Node type definitions. Required CI checkouts need the complete locked
TypeScript dependency installation, not just a compiler on `PATH`. No host
diagnostic or 24-hour qualification started after either failed CI.

PR note: Package WP-825 (open); Tier guarantee; Behavior: join native background
owners before the maintenance drain and CLEAN boundary; Checks: real baseline
crash-boundary refusal, passing release recovery/replay regression, 798 scoped
tests and 15 acceptance checks; Compatibility: unchanged public maintenance
receipts, durable bytes, command ordering and durability profiles; Updated
handbook: backup and restore, this handoff and live observations;
Hazards/follow-ups: final-source full CI and host repetition, the unresolved
earlier archive-publication failure, native eligible-history bytes and complete
lifecycle qualification. No 24-hour run has started.

## WP-825 final native CI and scratch isolation

Full CI passes on frozen native revision `f7402c87`, including workspace tests,
four-language operational conformance, lifecycle preparations and release source
installation. Its clean source and CI-log digest are retained privately. The
unchanged 900-second sink-outage diagnostic failed on the required host:
delivery remained degraded after access restoration and the last two windows
overflowed the latency histogram. This is diagnostic work, not the seven-fault short
rehearsal or a 24-hour qualification.

A separate failing test reproduces shared testkit scratch cleanup deleting
a directory whose PID namespace is unknown. Linux scratch names and sweeps now
bind the actual kernel PID namespace. Same-namespace dead-session cleanup,
panic cleanup and kept-evidence behavior remain required; foreign namespace
and legacy names of unknown ownership survive. The change affects test fixture
lifecycle only. It does not explain the earlier archive-publication failure:
that storage suite already has its own namespace isolation.
Frozen integrated testkit revision `4fbb44a5` also passes full CI and release
source installation. This receipt excludes the later checkpoint-wait changes.

PR note: Package WP-825 (open); Tier internal implementation within the
guarantee package; Behavior: isolate testkit stale-scratch cleanup by PID
namespace; Checks: baseline deletion reproduced, all 42 testkit tests and all
13 scoped acceptance checks pass; Compatibility: no database/application API,
storage format, workload, native validation or test-deadline changes; Updated
handbook: contributing, this handoff and live observations; Hazards/follow-ups:
unknown legacy scratch directories need deliberate owner cleanup, and native
delivery/latency repair, byte measurements and full short rehearsal remain required.

## Captured source validation during checkpoint waiting

The native retention worker previously left an existing read-only source
candidate unadvanced during its checkpoint wait. It now advances one bounded
page between the existing 100-ms wait slices, stopping page work at exact end.
It does not capture another root or request a writer turn. Writer admission,
deletion, independent checkpoint licensing and full writer-residence rest
remain in the ordinary step. Cancellation and validation errors stop the work;
stale qualification discards the candidate and exact end releases its reader.

This repairs a separate native coupling, not the observed host failure: the
failed diagnostic recorded zero logical checkpoint-wait holds. Its final lane
was cancelled without an incident, removed 113,922 source receipts and recycled
one extent, while incoming source history advanced faster. All 90,000 commands
and 360,000 reads completed without retries. Final sink health remained degraded;
state/archive reconciliation did not run after the controller refused recovery.
These results cannot qualify a rehearsal or prove a cause for the latency.
Complete canonical archive validation on a private copy observes 142,532
records and 36,980,305 encoded bytes, below the 1-GiB quota. The record count is
13,004 below the expected acknowledged workload; complete identity/payload
reconciliation remains unproved. Original failed data is preserved.

PR note: Package WP-825 (open); Tier guarantee; Behavior: continue captured
read-only validation inside the existing checkpoint wait; Checks: baseline
writer-admission regression fails, three deterministic worker scheduling tests
and eight storage drain/validation tests pass; Compatibility: unchanged public
interfaces, ordering, holds, durable formats, fsyncs, page/deletion bounds and
writer rest; Updated handbook: this handoff and live observations; Hazards and
follow-ups: scoped acceptance, final-source full CI, host delivery/latency
diagnosis, native byte accounting and the complete short rehearsal remain
required. No 24-hour run has started.
The retention change's scoped acceptance passes all 13 checks and all 1,840
tests, with ten existing skips. Subsequent archive timing instrumentation is
separately tested below and still requires integrated final-source acceptance.

## Native archive cost observations

Observed outage preparations accept `--population-probe-seconds 900` or
`--population-probe-seconds 1500` with `--observe` and one of `--sink-outage`
or `--consumer-outage`. The longer bounded mode leaves a complete five-minute
normal window after a pause of five minutes and the existing recovery budget
of ten minutes. Every measured window retains its rate, concurrency and
observation checks; normal windows retain p95 below 100 ms and p99 below 250 ms.
The shorter mode can refuse `probe_normal_window` despite meeting the recovery
budget. Both are preparation evidence; neither qualifies the 24-hour campaign.

The native worker now emits bounded timing observations for archive admission,
acceptance and per-record commit beside its existing primary-writer stages.
Each graph owns its counters; restart starts a new census and the worker's
retirement prints the final snapshot. A read-only timing handle retains no
archive, reader, writer lease or delivery capability. It cannot acknowledge
an event, grant reclamation, or turn failed delivery into healthy status.

Counters measure attempts, including refusals and equal retries. Commit timing
is included in acceptance timing; an equal retry may also call admission from
acceptance. Do not sum overlapping stages or treat attempt counts as deliveries.
Archive creation/initial validation and startup timings are outside these
per-record observations. Two-phase Immediate commits, sync boundaries, quota,
deduplication, fault duration, offered load and all qualification gates remain.

PR note: Package WP-825 (open); Tier guarantee; Behavior: native archive timing
observations for diagnosis; Checks: missing-observation test fails before the
implementation, real archive retry/refusal/accounting and concurrent-counter
tests pass; Compatibility: no application protocol, health label, durable
format or persistence-policy change; Updated handbook: this handoff and live
observations; Hazards and follow-ups: final-source acceptance/CI and required-host
delivery/latency attribution remain required. Diagnostic counters alone do not
qualify a rehearsal.
All 33 outbox tests, both server diagnostic-format tests and all-feature Clippy
for the three affected crates pass. These receipts do not replace final-source
CI or the failed host recovery/latency gates.

## Native provider-budget observations in preparation

Observed population preparations also collect the daemon's existing
`exact_provider_lifecycle` census. The pool's charge is the sum of fixed owner
bytes, retained owned bytes and reserved unpublished work, checked against the
unchanged aggregate 1-GiB limit. This is independent of RSS and the conservative
whole-process virtual-address-space observations. A smaller VmSize does not
prove provider retirement or complete provider charge accounting.

The bounded collector starts at the load-ready boundary and reads only new
complete diagnostic lines. Each five-minute window needs actual samples; gaps
above 75 seconds, unavailable counters, counter rewinds within one pool,
inconsistent ownership, changed log identity and over-budget charges refuse the
preparation. Allocation count is a live gauge that may fall during retirement;
released allocations and released owners are cumulative counters. The native census is due every 60 seconds. Partial lines remain
pending; old seeding samples cannot fill load-window coverage. Output contains
numeric counters and closed labels, with private exclusive publication.

The observation explicitly does not qualify the complete campaign resource
model. Transient process accounting, eligible-history bytes, all lifecycle
faults and the six final fault-free hours remain required. This collector
changes preparation evidence only; database ownership, admission, requests,
durable bytes and application interfaces are unchanged.

PR note: Package WP-825 (open); Tier internal; Behavior: independently collect
native aggregate-provider charges during observed preparations; Checks: missing
collector regression, twelve provider-accounting/coverage/log tests and all
78 lifecycle checker tests pass; Compatibility: existing native diagnostic
format and all workload/service gates preserved; Updated handbook: this
milestone handoff; Hazards and follow-ups: real final-source preparation and
complete campaign resource accounting remain unproved. No 24-hour run is started.

## Bounded shutdown evidence collection

The extent observer streams the closed daemon diagnostic log instead of loading
the whole file. Its fixed limits are 16 MiB total, 4 KiB per complete line and
262,144 lines. It retains at most three copies of each shutdown record kind,
allowing the existing one-or-two-copy check to reject duplicates. A changed,
replaced, linked, oversized or incomplete log refuses collection.

The previous 1-MiB whole-file limit rejected a complete native shutdown census
behind an otherwise bounded volume of diagnostics. This repair changes evidence
collection only. Native deletion/reuse counters alone still cannot qualify a
particular physically reclaimed generation: its disappearance must have been
observed on this volume. Missing or refused maintenance still fails, and measured
extent observations survive a terminal maintenance refusal.

PR note: Package WP-825 (open); Tier internal; Behavior: bounded streaming of
shutdown evidence; Checks: the larger-log regression fails before the repair;
all 81 lifecycle checker tests pass, including log replacement, size, line-count,
partial-line and custody refusals; Compatibility: no runtime, durable format,
application interface or qualification threshold change; Updated handbook: this
milestone handoff; Hazards and follow-ups: final-source checks and an actual
complete short rehearsal remain required. No 24-hour run is started.


## WP-825 bounded native outbox admission

Required-host preparation exposes a native writer bottleneck: outbox status
updates materialize the entire published journal suffix under the writer gate.
The Statistics operation waits behind that work because its audit is durable
and mandatory. A bounded numeric diagnostic observes a 4.075-second Statistics
call and a 3.528-second outbox suffix materialization. The original five-second
observer deadline and every workload gate remain unchanged.

Prepare a large prefix with the existing asynchronous checkpoint before opening
the outbox transaction. Wait for checkpoint materialization and unfinished
sealer/spare preparation outside the writer gate and checkpoint state lock,
then reenter the existing FIFO writer and revalidate the actual suffix. The
closing scan admits at most 256 transitions and 1 MiB. Retain the selected
Immediate transaction, exact status CAS and reciprocal validation, receipt
capture, membership publication, uncertainty fencing and individual claims.
Successful completion groups retain their sixteen-entry limit. No new journal
encoding, batching worker, public control or weakened audit is introduced.

The native large-prefix regression fails before repair and passes afterward;
it explicitly pauses materialization while another durable audit completes.
The spare-preparation regression also fails before repair and passes afterward.
All 1,330 active storage tests pass with two unchanged ignored tests; all 59
architecture checks, storage/server Clippy and thirteen metadata gates pass on
the required host. The unused unbounded-drain helper is test-only; focused
checks and Clippy pass after that annotation. Unchanged measured preparation
remains pending. The separate source-byte accounting freshness gap
still gates rehearsal readiness. Preserve failed populations and earlier
attempts; do not substitute relaxed observation deadlines for a native repair.

PR note: Package WP-825 (open); Tier guarantee; Behavior: bounded native outbox
admission with checkpoint preparation outside the writer gate; Checks: both native
negative/repaired regressions, 1,330 active storage tests, 59 architecture checks,
storage/server Clippy and thirteen metadata gates on the required host; Compatibility: unchanged application interfaces, durable
encodings, Immediate profiles, CAS and receipt semantics; Updated handbook:
this milestone page; Hazards/follow-ups: source-byte freshness, unchanged preparation, populated
recovery and final-source CI.
The 24-hour qualification run remains unstarted.

The first shipping candidate for bounded outbox admission builds normally but
fails initial archive drain after 41.7 seconds. The event observer exits with
`observer_ack`; the server does not panic and closes cleanly. That candidate's outbox adapter
held the server's shared operational-port cell throughout checkpoint admission,
although the lower storage layer releases the writer gate. Event acknowledgement
needs that cell, so off-gate checkpoint waits can still exclude it.

The activated server bridge now owns a restricted native outbox capability.
Forward all six outbox mutation paths through the existing storage implementation without
holding the operational-port cell. Retain the cell for other existing mutable
ports, and retain the pure-read handle's refusal of mutation. The bridge regression
explicitly holds the cell across an outbox transition and fails against the old
composition with its deadlock watchdog. Repaired C3D checks pass; unchanged shipping
preparation remains required. This finding does not close the source-byte freshness
gap or qualify a 24-hour run.

The native outbox capability remains move-only in its own server sharing cell.
Only that cell is held across outbox admission; event consumer operations use
the original cell. Both cells share one fail-closed poison flag, published on
unwind before releasing the local lock. Deterministic tests cover lock exclusion
and poison refusal in both directions. Writer sequencing and pure-read authority
remain unchanged. The final move-only composition passes all four bridge
regressions, both checkpoint regressions, the full server suite, the compile-fail
move-only capability proof, 59 architecture checks, storage/server Clippy and
scoped metadata acceptance on C3D. The original bridge regression fails before
repair, and the first draft also fails the poison-refusal regression before that
policy is restored. No accepted ordering, durability, archive protocol, public
interface, fixture or qualification gate changes. Unchanged preparation, populated
public restore and final-source full CI remain required.


PR note: Package WP-825 (open); Tier guarantee; Behavior: native outbox checkpoint
admission releases the server operational-port cell, preserving one move-only
dispatcher capability and shared poison refusal; Checks: negative/repaired bridge
and poison regressions, four final bridge tests, two checkpoint regressions, full
server suite, compile-fail capability proof, 59 architecture tests, storage/server
Clippy and scoped metadata acceptance on C3D; Compatibility: unchanged application
interfaces, reviewed fixtures, journal formats, selected Immediate durability,
FIFO writer policy, CAS, receipts and uncertainty fencing; Updated handbook: this
milestone page; Hazards/follow-ups: unchanged shipping preparation, source-byte
freshness, populated public restore, final-source full CI and the complete short
rehearsal. WP-825 remains open and the 24-hour run remains unstarted.

The ordinary release candidate `a605ec69` clears initial archive drain and event
acknowledgement, but unchanged preparation fails `source_bytes_sample_gap`
after 1,510.8 seconds without recorded competing host activity. Its accounting
refresh requires an identical completed eligibility interval. Expanded
eligibility and reclamation of that old interval can prevent a fresh count
while unrelated source-tail proof continues.

The native observer now starts a paced bounded capture under the existing
finite checkpoint license in both cases. It reuses only a covered qualified
prefix and canonically validates new receipts or missing accounting before
publishing exact bytes. Incomplete counts stay unknown. Reclamation authority,
holds, two validation pins, cadence, freshness and workload gates remain intact.
Four negative/repaired regressions and the nineteen source-byte, twelve
continuation and five checkpoint-license tests pass on C3D. The accounting
candidate also passes all 1,334 active storage tests with two unchanged ignored
tests, including 59 architecture checks, and storage/server Clippy. Final-source
acceptance and CI remain required. The recovery latency spike is still unexplained;
unchanged preparation, populated public restore, final-source CI and the complete
short rehearsal remain required before starting the 24-hour run.

A second native regression establishes that event-consumer checkpoint waits
can also retain the ordered writer and block durable audits. Its production
bridge separately retains the shared operational-port cell. Consumer admission
now waits outside the FIFO writer and uses the existing bounded checkpoint
preparation. A separate move-only consumer cell forwards the original native
transitions and protected operations, preserving shared poison refusal,
transaction-current authority, leases, source holds, receipts and durability.
Both negative regressions fail before repair. Both repaired consumer checkpoint
tests, three consumer bridge tests, both outbox checkpoint tests, four outbox
bridge tests, the compile-fail capability proof, 59 architecture checks and
storage/server Clippy pass on C3D. Frozen-source measurement must establish whether
this repair resolves the recovery latency spike.

PR note: Package WP-825 (open); Tier guarantee; Behavior: bounded qualified
source-byte capture after eligibility growth or accounting retirement, and
consumer checkpoint waits release the writer and shared operational-port cell;
Checks: paired negative/repaired native and bridge regressions, accounting
candidate's 1,334 active storage tests, source-byte/continuation/license suites,
consumer and outbox regressions, architecture, Clippy and capability proof on
C3D; Compatibility: unchanged application interfaces, authority, ordering,
durability, journal encodings and reviewed fixtures; Updated handbook: this
milestone page; Hazards/follow-ups: final-source acceptance and CI, unchanged
preparation, latency qualification, populated public restore and complete short
rehearsal. The 24-hour run remains unstarted.

The frozen `883e1987` preparation clears initial archive drain and records
24 complete in-load source-byte captures across five load windows. It completes
150,000 commands and 600,000 reads with no retries or post-duration work, but
fails `probe_latency`. Two fault-free windows exceed the histogram's
250-millisecond range. This establishes accounting progress, not latency
qualification. Populated public restore and dependent full CI remain unstarted.

A separate diagnostic replay reproduces the latency refusal. Cloud disk metrics
show recovery reaching the 500-GiB `pd-balanced` disk's sustained write-IOPS
ceiling; later normal windows also show substantial I/O queues. A sampled native
stack establishes an outbox engine flush holding the writer gate while an audit
and a consumer wait. These observations do not yet assign the complete latency
failure to one cause. Durability profiles and workload gates remain unchanged.

The next consumer-isolation diagnostic fails during seed, before its planned
pause or measured load. Its observer returns `observer_consumer`, so this is
neither an isolation result nor a successful preparation. Observer consumer,
acknowledgement and status failures now emit one bounded diagnostic containing
only the closed call stage and validated error class. Peer text, payloads,
incident identifiers and context are excluded. The original fatal return codes
and refusal behavior remain intact; the diagnostic adds no retry policy.


Two diagnostic isolation controls complete all 150,000 commands and 600,000
reads without retries or post-duration work. Pausing the consumer after load
second 600 produces passing later windows, but also freezes its retention floor.
Keeping the consumer active while disabling retention also produces passing
normal windows; that control disables source accounting as well. These results
narrow the native investigation to retention-related work without assigning one
exclusive cause. Neither control is qualification evidence or a campaign setting.
Two regressions separately reproduce the retention worker's zero-delay polling
and its missing observation of actual charged source rest. The native worker now
observes a bounded slice of that existing rest while captured read-only pages
continue progressing. Ready proof work, actual durable deletion, writer-residence
rest, admission and source validators retain their existing rules. Repaired worker,
source-accounting and architecture tests and Clippy pass on C3D. This repairs
bounded maintenance work; its latency effect still needs the final unchanged
preparation and all remaining milestone gates.

The frozen `120b1b0f` release builds, but unchanged preparation stops during seed
archive drain with a consumer `TRANSPORT_UNAVAILABLE` diagnostic. All 65,536
tickets are seeded; measured load never starts. The charged-rest repair therefore
has no qualified latency result. A separate diagnostic replay makes consumer
progress and captures a reactive-module read waiting on the operational bridge's
write lock. The narrow-reader correction keeps the original immutable
repository read and poison refusal; a real-storage barrier test checks that this
read can finish while the operational cell remains occupied. Native exact-module
and absence checks also prove that reading adds neither mutation-gate tickets nor
audit records. The original locked reader fails its barrier regression; repaired
bridge, native, architecture, Clippy and scoped metadata checks pass on C3D. The
short diagnostic nevertheless refuses latency. Neither diagnostic
establishes the complete failure cause. Preparation, populated public restore,
final-source CI and the complete short rehearsal remain required before the
24-hour run can be ready.

The next frozen release, `912f974a`, reaches measured load but fails the unchanged
source-byte observation gap after its last complete sample at second 867.6.
Load stops after 962.7 seconds; the server and consumer exit cleanly. A corrected
deterministic native schedule reproduces an older completed mutation proof
discarding newer unfinished accounting through the existing fail-closed pin
sharing rule. The native scheduler now finishes the already captured accounting
pages before offering that proof's pin, preserving both existing pins and every
sharing check. This grants no writer turn or new snapshot. The scheduling repair
passes 34 accounting tests, five checkpoint-license tests, 24 worker tests,
59 architecture tests, Clippy, formatting and scoped metadata acceptance on C3D.
Unchanged preparation, restore, final-source CI and the complete campaign remain
required; no qualification result is implied by these focused checks.

The campaign control-file helper now requires both original workers to acknowledge
quiescence before publishing the shared next-generation release. It keeps the
acknowledgements until the observer reports the new generation and the load makes
a subsequent command completion, avoiding acknowledgement republication during
resume. A declared restore permits exactly one incarnation advance and retains
its marker until both workers resume. Seventeen focused controller tests pass on
C3D. This component does not implement or qualify the complete seven-fault runner.

The unchanged `2bc25f14` preparation now completes all 150,000 commands and
600,000 reads with zero retries or post-duration work, and reconciles all 65,536
tickets. Source accounting supplies 25 complete observations, five per load
window, with a maximum gap of 66.8 seconds; logical and physical observations
also cover every window. This supports the accounting scheduling repair under
load. Preparation still refuses `probe_latency`: the sink finishes recovery
near load second 607.8, and both later normal windows exceed the latency limits.
The 900–1,200-second window has p95 88.6 ms and p99 beyond the 250-ms histogram;
the 1,200–1,500-second window has both percentiles beyond that range. The provider
charge peaks at 8,913,408 bytes within the existing 1-GiB limit. The collector's
component observations do not qualify the complete resource model, and process
exit allocator accounting remains unknown. Populated restore, full CI and the
complete short campaign do not follow this failed preparation.

Private campaign handover now uses a one-shot receipt from the Rust load worker
after a newly acknowledged command, rather than a controller-supplied completion
counter. A read completion or generation change alone supplies no confirmation.
The controller can pause the original observer first while keeping load active
for a restart fault, then drain load before changing the shared release. Credential
rotation reconciles the actual quiesced acknowledgement prefix and current ticket
revision while retaining revoked-bearer refusals and original-identity replay.
The original worker fails the missing-confirmation regression; the revised worker
passes all 37 TicketDesk tests, and 194 lifecycle harness tests pass on C3D along
with formatting, Clippy and scoped metadata acceptance. These private controls
introduce no application API or durable-format change. Complete fault execution,
process replacement accounting and campaign qualification remain outstanding.

The native payload-read investigation independently reproduces a publication
lock held across storage I/O: the canonical command read succeeds, but a
deterministic hook cannot acquire the index writer guard before payload loading.
The repaired read selects one immutable metadata entry and at most one cache
candidate under that guard, then resolves the same pinned storage authority
after releasing it. Exact frontier, canonical-byte or checked-stub identity,
checksum, metadata and member checks remain mandatory. A completed cold read
may populate the bounded cache only while that exact metadata entry still
belongs to the index; replacement or pruning prevents stale reinsertion.
The existing read-root tracking continues to protect physical history extents.
Writer-side published/unpublished probes retain their continuous guard.

Eleven focused native tests pass on C3D, including cold and cached command reads,
provenance and audit lookup, index replacement during resolution, and refusal
of a corrupted successor row despite an older cached payload. The full storage
suite, all 59 architecture tests, Clippy and formatting also pass on C3D. The two
existing ignored storage qualification tests remain unrun. A lint-only move of
an unchanged test helper preserves the production source used by the full suite.
Scoped metadata acceptance also passes. This demonstrates the lock-scope
defect and its repair; it does not establish the entire cause
of the measured latency refusal. Cache limits, application interfaces, ordering,
durable formats, workload rates and qualification gates are unchanged.


The frozen native repair's first unchanged preparation stops before measured
load: the seed acknowledges all tickets, but the event consumer reaches its
existing 30-second transport deadline. The server remains alive and closes
normally during cleanup. A debugger-instrumented seed diagnostic observes all
selected seed events without duplicates and does not reproduce that timeout;
it supplies no preparation or latency qualification. An uninterrupted repetition
uses the same frozen binaries and preserves the failed attempt. It also reaches
the same consumer timeout before load. Native gate residence and history decoding
remain under investigation; the harness adds no
retry, delayed consumer registration or relaxed deadline.

The private campaign process roster now preserves earlier exit RSS peaks when
an exact role is replaced. It admits a successor only after the original owned
child's expected exit, kernel high water and finite inherited birth bound are
available. The same exact descendant census runs with the currently live roles,
including periods after explicit retirement. A short-lived operator remains
accounted for even when it exits before the first live sample. Frozen hard
address-space ceilings cover steady workers and one serialized launch's larger
inherited birth ceiling; these are conservative bounds, not live allocation.
Nine roster tests, 12 persistent-owner tests and all 204 lifecycle tests pass on
C3D, including actual child replacement and operator exit accounting. Continuous
sampling, all actual campaign launches, native observation epochs and the full
fault controller remain unproved. The component continues to report incomplete
resource-model qualification, and the 24-hour run has not started.


The roster's scope, internal classification and metadata acceptance also pass
on C3D. A separate native regression then reproduces cache eviction based only
on commit sequence: reading the oldest retained payload does not protect it
from the next new publication, so an active historical reader decodes it again.
The original test fails at that assertion. The repair evicts the least recently
verified payload while keeping the same 64-entry and 64-MiB ceilings. Current
canonical or checked-stub identity and metadata checks precede every refresh;
replacement or pruning still prevents stale reinsertion. An already captured
candidate survives eviction but cannot hide corrupted current authority.
All 13 focused native tests, the full storage suite, 59 architecture checks,
Clippy and formatting pass on C3D, including both lookup paths and deterministic
eviction interleavings. Final metadata acceptance and an unchanged measured
preparation remain required. This repairs a native eviction defect without establishing it
as the entire cause of the consumer timeout or latency refusal. No workload,
request deadline, format, authorization or qualification gate changes.


A deterministic process barrier also exposes a private launcher race: its proof
pipe closes before the target executable is installed. The parent can therefore
capture the launcher's virtual-memory high water and mistake the target's lower
post-exec value for a regression. The launcher now leaves the non-inheritable
proof descriptor open until exec closes it. The original assertion fails before
the repair; all 13 persistent-owner tests, nine roster tests and the complete
205-test lifecycle suite pass afterward on C3D. This corrects launch observation
without relaxing the memory checks. Integrated resource qualification remains
unproved.


The cache and launcher fixes subsequently pass final scoped metadata acceptance
and are committed together. A fresh unchanged preparation builds its ordinary
release binaries successfully, then reaches the same native consumer deadline
before load. Seeding acknowledges all tickets in 30.8 seconds. No restore or
full-CI pass follows that refusal. A separate repetition, configured to attach a
debugger only after the original consumer failure, instead seeds in 116.1 seconds
and consumes all 1,024 selected seed events with zero duplicates. No debugger
attaches; the diagnostic is stopped before load and proves no preparation gate.

Source inspection identifies another native excess: protected replay authorizes
up to 1,024 candidate events before truncating to the requested visible prefix,
which is 32 events in this workload. A candidate repair moves that existing
admitted-prefix cut into evaluation. Denied rows must remain in the fence and
consume bounded candidate work; insufficient visibility must still examine the
full bounded prefix. Capability validation, one pinned policy snapshot, current
row and relationship checks, and the backend's final transaction-current
revalidation remain mandatory. Other lease operations continue to authorize all
selected candidates. The eager-walk regression fails at its intended assertion
before the repair. Seven prefix regressions, the full query-executor suite,
ten row-policy conformance tests, protected-consumer coverage, the server suite,
Clippy and formatting pass on C3D. Final metadata acceptance and an unchanged preparation
remain required; these results establish no sole-cause claim.

Final scoped acceptance subsequently passes all 12 checks, and the prefix repair
is committed. Its unchanged preparation builds ordinary release binaries, then
fails the consumer transport deadline before load. Seeding acknowledges all
tickets in 30.2 seconds. The actual TicketDesk consumer uses an unprotected
event-window path; the protected-replay repair is outside this failure path.

A diagnostic reproduces the original consumer failure before debugger capture.
The consumer is waiting for checkpoint completion during its native transition.
Another diagnostic enables existing bounded checkpoint counters: 14 checkpoints
start and complete without visible consumer progress, with selected batches
growing from about 1 MiB to 63 MiB. Its distinct protocol refusal is preserved.
The admission loop gives up its FIFO place during each wait, then checks a newly
replenished suffix. This is a native writer-admission issue; neither larger
deadlines nor a slower harness can establish sustainable progress.

ADR-0282 (`adr/0282-finite-direct-mutation-checkpoint-handoff.md`) proposes
a finite FIFO handoff after the earlier checkpoint. It would permit bounded
native backpressure on later writers while one fixed captured tail is
materialized, preserving reads, authorization, durability and formats. This
proposal requires exact human acceptance before implementation. Source
reclamation would gain no reservation or priority. Existing latency, workload
and qualification gates remain required; the proposal is not available behavior
and establishes no preparation success or 24-hour readiness.


Main integration brings in the accepted extent pacing and receipt-hash changes
and checkpoint root validation once per ordinary batch. Follower acknowledgement
validation is now ADR-0283, preserving its accepted decisions and original
acceptance history; main's extent-pacing record retains ADR-0280. The maintainer
approved this identifier reconciliation separately.

The integrated checkpoint path preserves borrowed canonical validation and
journal history stubs. A source cut retains its complete validators and carries
its exact new retained floor into following frames. A mixed audit/cut/audit
regression fails without that carry and passes with it; the ordinary-batch
regression, full storage API/backend suites, Clippy and formatting pass on C3D.
These component results remain distinct from final integrated acceptance and
measured preparation. ADR-0282 remains proposed; repeat unchanged preparation
on the integrated source before deciding whether its handoff is needed.

Check main at active work-session boundaries and before freezing CI or rehearsal
sources, integrating at bounded implementation milestones. Record the source
revision for each new run; preserve sources and artifacts of in-flight and
completed runs. An integration requires its own validation and cannot inherit a
previous revision's qualification.


The following main update includes accepted ADR-0281 and completed WP-829:
transaction-level root validation for replay and direct-write suffix draining.
Bootstrap source custody is now ADR-0284 under separate identifier-only human
approval, preserving its original acceptance and complete Decision text.
Integration preserves bounded streaming recovery and complete source-cut checks;
real replay/drain tests cover a cut followed by another audit frame.

The earlier integrated source's unchanged preparation reached four complete
five-minute load windows before failing process-inventory accounting. All offered
operations in those windows completed without retry. The run remains failed;
one window's p95 exceeds the normal ceiling, and neither full reconciliation,
populated restore nor full CI ran. Preserve its frozen evidence and identify the
resource refusal without relaxing census bounds or workload gates. Repeat
preparation on the newly validated main integration. ADR-0282 remains proposed;
no finite-handoff implementation or 24-hour readiness is established.


Preparation now preserves a bounded private `resource-refusal.json` when process
memory accounting refuses a sample. It distinguishes a changing task census,
untracked descendant, changed child set and ownership-tree refusal using only
closed reasons and roles. The original refusal still fails the run, even if
cleanup also fails; three census attempts and all existing process/thread limits
remain unchanged. The report includes no process identities, paths or input
text and never qualifies the resource model. All 209 controller tests pass on
C3D, including actual load-child cleanup and diagnostic redaction.

### Finite campaign worker and maintenance calls

The administrative owner also runs a fixed set of Rust workload checks:
rotation, independent state reconciliation, archive reconciliation and the V2
ungranted-command refusal. Each report has a closed numeric/Boolean shape;
these checks use the same serialized child slot, deadlines, output bounds,
process census and retained exit peaks as public CLI calls. A per-call
credential replaces the existing CLI credential flag and is never written to
the numeric operator record.

A native backup start, including an uncertain start, registers its original
maintenance operation identifier. Only a read-only `backup.operation` call for
that exact registered identifier may retain the CLI's exact native
`transport_unavailable` or `tls_connection_failed` response while the offline
listener is unavailable, as required by ADR-0050. Each new connection still
performs mandatory peer verification. That response stays
unknown; it supplies no successful maintenance result and never permits another
backup or restore submission. Other failures remain fatal. At most two original
maintenance operations and 64 total administrative children are admitted.

These are controller components; they do not qualify the complete campaign,
resource model, final-source CI or the 24-hour run. C3D validation is pending.

PR note: Package WP-825 (open); Tier internal; Behavior: finite workload checks,
per-call credentials and original-operation maintenance polling; Checks:
pending C3D; Compatibility: public interfaces and durable formats unchanged;
Hazards: complete fault execution and whole-fault recovery proof remain
required. Updated page: this handbook page.


### Restore a pruned backup after every source owner closes

The ordinary release preparation completed its unchanged 1,500-second load,
all five original windows and final state/archive reconciliation. A separate
native restore diagnostic then identified a retained-history ownership defect:
offline restore copies the journal artifacts but retention-watermark validation
has no resolver for their receipt locators after the source store closes. It
returns `Unavailable` despite a checksum-verified backup. A small regression
using an actually pruned command history and a recovered journal receipt
reproduces that exact failure with no live source resolver.

Restore now retains a resolver for its own verified target extents through
watermark stamping and fencing reads. It keeps checksum, history, watermark,
tombstone and immutable-source checks intact; it changes no format or public
error handling. Required-host positive storage tests, Clippy, formatting and
scoped acceptance are pending.

The completed-load barrier separately passed all 39 lifecycle tests, Clippy,
formatting and scoped acceptance on the required host. The finite operator
integration passed all 256 controller tests and scoped acceptance there. Earlier
candidate notes marked those checks pending; these results close only those
component checks.

A distinct instrumented startup diagnostic reproduces the initial public
`storage_unavailable` before recovery-route installation: opening TCP does not
prove recovery admission. Both original failed attempts remain preserved. An
explicit native recovery-admission signal and fresh public restore proof remain
required; no mutation retry, fixed startup delay or relaxed recovery deadline
qualifies them.

PR note: Package WP-825 (open); Tier guarantee; Behavior: retain the restored
journal resolver during offline retention normalization; Checks: actual native
semantic negative on the required host, positive checks pending; Compatibility:
public interfaces, encodings and reviewed fixtures unchanged; Updated pages:
Sustainable TicketDesk milestone and live observations; Hazards/follow-ups:
recovery admission, fresh populated public restore, complete resource model,
final-source full CI and the real short rehearsal remain open. No 24-hour run
has started.

The corrected regression passed alone, but the parallel storage run exposed a
fixture identity collision: other tests use the same database identity, so their
live resolvers affect the expected missing-owner error. The regression now uses
its own deterministic identity and retains the exact missing-owner assertion.
The failed parallel run is preserved; the full rerun and acceptance are pending.
Both stopped recovery routes also have executed negative regressions: generic
restore admission and the exact admitted-operation retry remain open after stop.
Their native closure fix and readiness diagnostic are still under validation.

### Campaign assembly under validation

The private campaign control preparation now creates fresh, least-authority
controls and an exact-parent successor closure, without seeding or starting load.
It uses supported 28-hour capabilities and an 8-GiB archive quota. The controller
draft starts the original consumer before the 65,536-ticket seed, observes actual
archive drain, and retains the original load, observer and offered clock through
all seven planned faults. Quiesced state reconciliation reads only the original
acknowledged witness prefix; successor authorization testing performs no extra
application mutation. These components do not yet qualify a measured rehearsal.

File observations cover live data, journal, archive, backups, derived state and
all control/evidence files. Each file contributes the larger of apparent length
and allocated 512-byte blocks. Retained pre-restore data and projections count
as backup copies, never as physical reclamation. The exact open archive object
remains counted during its controlled permission outage. Global sampling gaps,
file limits and window maxima survive process and history replacement. Native
physical, logical and source-history trends remain independent and overlapping
bases are never added. Complete reconciliation and actual native fault execution
remain open; fixed protocol fixtures and file tests prove only their components.

PR note: Package WP-825 (open); Tier internal; Behavior: private campaign
preparation, fault-dispatch and complete-file observation components; Checks:
required-host Python, Rust worker, formatting, Clippy and scoped acceptance
pending; Compatibility: public database semantics, durable formats and fixtures
unchanged; Updated pages: Sustainable TicketDesk and live observations; Hazards:
fresh public populated restore, final campaign entry point and independent
reconciliation, final-source full CI, and real short rehearsal remain unproved.

The campaign controller separates the measured interval from final offline
archive validation. The long archive worker is admitted only after the original
period ends and every measured child has a retained retirement record. Ordinary
workers and fault actions retain their existing 60-second ceiling. A measured
extent retirement requires both a disappearance observed before the period ends
and a complete native record from the same history incarnation. Startup records
and later cleanup cannot supply measured retirements. These controller components
do not independently establish a qualifying database rehearsal.

### Finalization integration candidate

The earlier native restore-owner fix passed the corrected regression, all 1,398
storage tests, Clippy, formatting, scope and scoped acceptance on the required
host. Its commit preserves exact tested source bytes. The recovery-admission
regressions pass, but the broader server run fails primary fencing under load
with TransportUnavailable; the cause remains under investigation. Neither a
populated public restore nor final-source CI is qualified by those results.

The controller now connects its original offered clock, seven fault actions,
measured-incarnation extent attribution and completed-load barrier. Final
consumer, actual projection values, outbox, original replay, full ticket state
and offline archive reconciliation remain explicit execution gates. Process-local
root resets are separate from persisted generation identity. Later cleanup earns
no measured retirement credit. Controller fixtures and syntax checks supply no
actual database rehearsal; independent complete resource and interference
reconciliation, frozen conformance/CI evidence and the final executable remain
required. No 24-hour run has started.

PR note: Package WP-825 (open); Tier guarantee; Behavior: integrate finite
campaign finalization and attribution; Checks: C3D validation pending for this
candidate, prior component receipts retained; Compatibility: application
interfaces, durable formats and reviewed fixtures unchanged; Updated pages:
sustainable TicketDesk milestone and live observations; Hazards/follow-ups:
unresolved promotion failure, final frozen inputs, full CI, complete resource
reconciliation and the real all-seven rehearsal remain open.

### Independent resource observations candidate

The resource reconciler reads immutable, private, bounded memory and disk
collector inputs and independently checks their original clock, whole-file
accounting basis, complete window coverage, retained process-role peaks and
serialized launcher birth ceiling. Control and evidence files remain bounded
separately from the seven public receipt resources. Host, launch/exit custody,
native history, interference and artifact evidence remain mandatory independent
checks; arithmetic fixtures supply no actual campaign qualification. C3D
validation is pending for this candidate. WP-825 remains open.

### Frozen owner-slot capacity candidate

The original 64-incarnation roster ceiling could not accommodate the existing
64 serialized operator slots together with the original workers and declared
replacements. Derive one finite lifetime cap of 76: one controller, four server
owners, five sampler owners, the two original workload owners and 64 operator
slots. Preserve serialized births, exact retirement, the request concurrency
limit and all frozen address-space ceilings. The regression exercises every
operator slot and declared replacement and refuses the next owner. C3D
validation is pending; no measured run is underway and no in-flight limit changes.

### Recovery admission validation candidate

The stopped generic and exact-retry recovery routes refuse new restore admission
while retaining only the service owner admitted before stopping. A fixed,
best-effort process diagnostic follows actual recovery-service installation;
it supplies no authority and cannot replace native admission. The controlled
promotion comparison passes the unchanged test for baseline and candidate and
all 26 parallel follower tests with bounded native child traces. No OOM kill
occurs. The original full-server TransportUnavailable failure remains preserved;
these comparisons do not establish its cause or erase it. Ordinary full-server
validation and scoped acceptance are pending for this candidate. Populated
public restore, final-source CI and the real all-seven campaign remain required.

### Independent closed owner inventory

Bind every declared server and sampler incarnation, both original workload
owners and each serialized operator to its private immutable launch proof and
Linux wait4 exit record. Preserve the killed server's signed exit status and
unknown exit allocator measurement; finite inherited hard limits supply only
conservative upper bounds. Reconcile dense operator order, expected exits and
original-clock durations against the terminal roster. Missing, extra, changed
or unbounded input refuses qualification. This component does not qualify a
resource model or campaign by itself; actual source, native history, host and
interference evidence remain required. C3D validation is pending.

### Original-clock observer observations

Retain each distinct message from the original observer with its actual age
and the controller's original monotonic elapsed time. Repeated polling adds no
message, stale or rewound input refuses, and post-period observations cannot
supply measured progress. The same original owner survives all faults. These
bounded private observations require independent comparison with native state,
actions and terminal reconciliation; they grant no campaign qualification.
Preserve the original failure if outage cleanup also refuses. C3D validation is
pending; the public populated restore and complete rehearsal remain required.

### Independent native and original-observer progress reconciliation

Check bounded closed observer and native inputs on the original offered clock,
including every five-minute window, finite sample gaps and exact final drain.
Keep owner-local sampler indices separate across declared replacements and
permit derived projection reconstruction only at the new history incarnation.
Preserve monotonic authoritative heads. Organization-zero event delivery can
legitimately be sparse between its matching updates; reconcile the exact final
event count, acknowledgements and actual live-query renewals independently.
These arithmetic fixtures grant no actual campaign qualification. Source,
owner/action custody, interference and artifact observations remain mandatory.
C3D validation is pending.

### Reserved-host interference sampling policy

Census known competing database, compiler and benchmark processes every five
seconds on the original offered clock. Account the exact roster owners first;
foreign matching programs are recorded and refuse the run. Bound all process
entries and input reads, preserve the kernel boot identity, and independently
require every window with at most 75 seconds between samples. Disclose the
frozen matching policy and exclusive host reservation with the result. This
observes honest process identities; it is not an adversarial execution monitor.
No absence flag is fabricated from an unobserved period. Component fixtures
qualify no actual campaign. C3D validation is pending.

### Pre-offer resource admission candidate

Replace provisional preparation limits before any measured campaign. Whole-file
ceilings are 64 GiB primary data, 16 GiB journal, 8 GiB archive, 32 GiB backups,
4 GiB derived state and 2 GiB harness/control evidence. Reserve another 8 GiB;
require at least 134 GiB available before population and refuse otherwise.
These include intentionally retained outcomes/provenance and free pages; they
cannot substitute for separate native history trends or physical retirements.
The observed 215,536-command preparation uses about 1.11 GiB apparent primary
storage; a simple linear projection to the full day's fixed offered workload
is below 48 GiB, not a guarantee of future behavior. The 64-GiB primary cap
leaves margin while the actual shorter rehearsal measures subsequent growth.
The observed derived footprint is below 65 MiB; 4 GiB includes rebuilds. Keep
32 GiB for the early fixed backup fault and retained predecessor copies, 8 GiB
for durable archive growth, and 2 GiB for bounded witnesses/logs and four frozen
binary copies. No ceiling may increase during a run; an exceedance fails it.

Require those immutable private binaries inside the campaign root so whole-file
control accounting includes them. Preserve frozen worker hard limits, the
27-GiB conservative allocator bound and the separate 1-GiB native provider
budget. Capture the observer's latest tuple once before reading its clock and
payload, preventing a new reader publication from mixing the two messages.
C3D validation is pending; this model admits inputs, never qualifies a result.

### One original offered clock candidate

The Linux Rust load worker publishes its CLOCK_MONOTONIC origin once using
the pinned safe rustix time wrapper already present in the dependency graph.
The campaign reader requires the closed V2 ready record, rejects absent, stale
or future origins, and adopts that exact epoch for fault actions and samples.
The workload schedules each offered second and the terminal boundary against
that same epoch. Replacement servers and observers cannot restart it. Legacy
preparation retains its closed V1 ready shape. This adds no native database
semantic, durable format, authorization bypass or dependency version. The time
feature is limited to Linux TicketDesk rehearsal code; first-party unsafe Rust
remains forbidden. C3D component validation is pending; this is not run credit.
