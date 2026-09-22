# WP-809: checkpoint-coupled journal tail latency

Identifier correction: this package was first registered locally as WP-806,
then WP-808. Main independently assigned WP-806 to the semantic write-path
comparison task and WP-808 to governance verification. WP-809 preserves this
package's original checkpoint scope and evidence.

**Open storage/journal package.** A registered asynchronous checkpoint makes
the Standard-profile writer drain journal durability and ordered publication
before continuing. This shared writer path can stall command acknowledgement
without any archive consumer. **WP-749 is now blocked on reduction of this shared
term**, per maintainer direction on 2026-09-21. WP-809 owns its repair;
WP-749 subsequently assesses its separate archive-presence gate by the defined
direct off/on measurement. WP-809 does not depend on WP-749.

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

The current source makes the connection explicit:
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

## Registration PR note

Package WP-809; registration change tier internal, planned implementation tier
guarantee. Behavior: no runtime change; give the shared finding independent
scope and an open exit gate. Updated handbook: this page and the linked archive
report. Checks: manifest brief, governance and documentation acceptance.
Compatibility: all existing bytes and guarantees unchanged. Hazards: the device
cause and propagated client queueing are not established; implementation and
its correctness/performance evidence remain future work.
