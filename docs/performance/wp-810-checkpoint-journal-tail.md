# WP-810: checkpoint-coupled journal tail latency

Identifier correction: this package was first registered locally as WP-806,
then WP-808 and WP-809. Main independently assigned those numbers to the
semantic write-path comparison, governance verification, and same-root revision
fence tasks. WP-810 preserves this package's original checkpoint scope and evidence.

**Open storage/journal package.** A registered asynchronous checkpoint makes
the Standard-profile writer drain journal durability and ordered publication
before continuing. This shared writer path can stall command acknowledgement
without any archive consumer. **WP-749 is now blocked on reduction of this shared
term**, per maintainer direction on 2026-09-21. WP-810 owns its repair;
WP-749 subsequently assesses its separate archive-presence gate by the defined
direct off/on measurement. WP-810 does not depend on WP-749.

## Established finding

The valid archive-disabled cell from the
[isolated checkpoint-wait comparison](wp749-archive-isolated-pair-2026-09.md)
contains 263,888 measured successful commands in 16,607 groups. Of those groups,
**123 wait over 50 ms** for their first journal receipt and **158 wait over
10 ms**. Every one has the actual checkpoint drain predicate set. With that
predicate false, the maximum first wait is only 4.036 ms.

| Archive-disabled observation | Value |
| --- | ---: |
| Raw command acknowledgement P99 | 88.347 ms |
| First receipt wait P99, checkpoint drain required | 56.855 ms |
| First receipt wait P99, no checkpoint drain required | 1.068 ms |
| Maximum first receipt wait | 160.026 ms |
| Maximum journal sync | 159.922 ms |
| P99-tail requests whose own group requires drain | 95.72% |
| Checkpoints, including warmup | 98 |
| Checkpoint transitions, minimum–maximum | 3,008–3,327 |
| Byte-triggered / transition-triggered selections | 98 / 0 |

The measured source makes the connection explicit:
`RedbStore` consults `async_checkpoint_in_flight` when setting the submitted
group's drain requirement; the commit writer's `POST_DRAIN_ALL` then waits for
completion. The real journal media writes bytes and performs durability sync;
the receipt completes before storage publishes the covered frontier and the
command response becomes eligible. A checkpoint's installation must preserve
the exact checkpoint-plus-suffix view and ordered publication. Removing the
drain without placing that guarantee elsewhere is not a permissible repair.

The trace names the blocking path. It does not identify the kernel/device
reason for a long sync, nor quantify this cost on every workload. The path is
shared by Standard-profile writers; the measured magnitude is from c32
write-only on the full app-baseline dataset. Hardened-profile behavior is not
inferred from this evidence.

## Evidence already sufficient to open the package

Frozen source: `be189cac4325c77303c5b4fe749ef9b73f4e4b42` plus the retained bounded
observational patch. Rust 1.98.1, thin LTO, one codegen unit, generic x86-64;
8-vCPU AMD EPYC 9B14 with required SHA-NI, AES, PCLMULQDQ and SSE4.2. The cell used
15 seconds warmup and 90 seconds measurement, with valid host checks and zero
errors, unavailable outcomes or dropped trace evidence.

`release/evidence/wp-749/archive-ack-isolated-20260921/summary.json` is the
existing immutable evidence bank. Its disabled-cell entry retains exact
source/binary/raw-input hashes, all checkpoint events, the slowest twenty
requests, group and sync/wait distributions, host checks and reconciliation.
The raw request arrays and traces have verified private copies. The evidence
stays at its original path so this handoff does not manufacture another run.

The repaired receipt counter includes the first wait. Deduplicated journal
sync totals and receipt-wait calls reconcile against the same trace. Accounting
still requires the [writer-census warnings](wp749-writer-census-defects.md):
wait and sync overlap, request-weighted shared group durations are not writer
wall, and the client time outside its own submitted group remains unattributed.
For the disabled P99 population, that remainder averages **72.272 ms**. Do not
assume it is all checkpoint-induced queueing.

The [archive report](wp749-archive-isolated-pair-2026-09.md#the-pre-sync-write-finding-survives-isolation)
separately preserves the enabled **141.387 ms journal write inside a
142.265 ms receipt wait**, with only 0.799 ms of sync. That is a distinct
pre-sync blocking observation on the same wait path, not evidence that archive
created the shared checkpoint dependency.

## Scope and next owner's work

The manifest entry owns a deterministic production-path reproduction,
guarantee-preserving disposition, recovery/crash coverage, and qualification
of any eventual mitigation. Existing ADR-0101/0104/0132 decisions remain in
force. No new codec, durable identity, cadence, bound, response ordering or
public surface is authorized by registration. A required decision change must
first receive exact-text review under AGENTS.md and D-003.

Registration needs **no further measurement** from the WP-749 investigation.
The later maintainer disposition makes this work WP-749's prerequisite and stops
that investigation; it does not start a campaign or reserve a host. A future
implementation must re-baseline its actual source/toolchain
under ADR-0239 and preserve ADR-0249's named-mechanism disclosure discipline.
No P99 threshold is invented here.

## Candidate repair, 2026-09-25

The local candidate keeps the ordered installation barrier while allowing the
existing bounded pipeline to run during checkpoint materialization. It changes
no checkpoint threshold, journal or capsule encoding, durability worker,
publication order, or acknowledgement requirement. Performance benefit is
**not yet measured**; WP-810 remains open and WP-749 remains blocked.

Implementation decisions within ADR-0101/0104/0132:

- A pending materializer alone no longer sets `requires_pipeline_drain`.
  The epoch-opening poll requests that drain after it observes successful
  materialization with unpublished predecessors. Completion racing a seal is
  observed at the next epoch opening. If the prefix is already quiescent, the
  existing poll can install directly before new apply.
- Installation still requires zero unpublished transitions and bytes, exact
  covered-root/frontier/hash checks, and publication of the rebased view before
  the covered extent is recycled. A completed checkpoint is not permission to
  acknowledge an unpublished command.
- At capacity pressure, the sole writer publishes the already-submitted FIFO
  prefix before retrying capacity or checkpoint installation. The staged next
  frame is excluded. Original completion tickets retain their results. The
  existing transition, encoded-byte, physical-extent and overlay bounds remain
  enforced; no larger queue or reserve is introduced.
- An observed checkpoint failure fences new apply immediately, even if earlier
  durable groups still await publication. Quiescence gates successful
  installation, not error detection.

The deterministic real-storage test failed on the old predicate and passes on
the candidate. Separate controls pause checkpoint materialization, the actual
journal sync, and ordered publication. The production coordinator test proves
that the next command can reach apply and seal while its predecessor's journal
sync is held, with both responses and application notifications withheld until
ordered publication. The fixture has no sleeps or replacement I/O results.

`tests/storage_recovery/checkpoint_pipeline.rs` also exercises the actual
256-transition unpublished ceiling and the 257th transition's backpressure,
original completion tickets after a pressure drain, and immediate reads of
entities, outcomes, commits, provenance and events. A checkpoint-failure arm
proves refusal of new apply with an outstanding predecessor. Six abrupt
process-exit cases cover before journal sync, durable-but-unpublished state,
checkpoint staging, checkpoint durability, installed-before-reclaim, and
reclaimed extent. Every case reopens twice and checks atomic, idempotent
authority without extra commits, events or outcomes. Before sync, an
unacknowledged complete frame may survive or disappear; both outcomes must be
atomic and stable on the second restart.

Local qualification exercised 1,315 storage/commit tests: 1,313 passed in the
broad run and two existing promotion/bootstrap crash tests reached its
120-second watchdog. Both passed in a serial rerun, together with all six new
schedule/recovery tests, without changing a deadline or assertion. One existing
test remains skipped by the normal runner. A fixture-only Clippy allowance was
corrected before the passing lint recheck. The initial failures remain in the
private qualification bank; they are not represented as a clean first run.

The bounded measurement plan is three counterbalanced control/candidate pairs,
c32 write-only, 15 seconds warmup and 90 seconds measurement per cell. Both
arms use the same source baseline, Rust 1.98.1, release flags and common
observation patch, with one client binary and distinct server digests. Retain
all attempts, exact raw P50/P95/P99 samples, throughput, checkpoint counts and
corrected waits. Reconcile disjoint writer stages against an independently
retained iteration sum and continuous span, with signed remainders; busy plus
idle is not wall time. A local debug smoke check verifies evidence plumbing
only and supplies no performance conclusion.

Next: finish scoped checks, run that comparison on an exclusively reserved
admitted host, complete merge qualification and obtain implementation/fixture
review. This candidate addresses the writer's dependency on a pending
checkpoint; it does not claim to eliminate the underlying device write stalls
or quantify their client-latency effect without measurement.

### Candidate PR note

Package WP-810; tier guarantee. Behavior: retain bounded pipelining during
checkpoint materialization and the ordered drain for installation; publish
existing predecessors under capacity pressure and fence observed checkpoint
errors immediately. Checks: deterministic real coordinator/storage schedules,
six process-crash edges, scoped storage/commit tests and path-triggered
acceptance; full merge checks and performance comparison remain pending.
Compatibility: existing durable bytes, public interfaces, thresholds and
authority guarantees remain unchanged. Updated handbook: this page. Hazards:
the device's long writes remain possible; no acknowledgement improvement or
WP-749 gate result is claimed before measurement and review.

## Registration PR note

Package WP-810; registration change tier internal, planned implementation tier
guarantee. Behavior: no runtime change; give the shared finding independent
scope and an open exit gate. Updated handbook: this page and the linked archive
report. Checks: manifest brief, governance and documentation acceptance.
Compatibility: all existing bytes and guarantees unchanged. Hazards: the device
cause and propagated client queueing are not established; implementation and
its correctness/performance evidence remain future work.
