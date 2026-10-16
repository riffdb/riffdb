# WP-825: retention and projection recovery findings

Package: WP-825. Tier: guarantee, under the existing accepted decisions.
This note records observed defects and a design boundary; it accepts no new
source of authority, backup format, validation shortcut or workload change.

## Offline pruning result

A genuine three-command database contained a fully applied, published projection
at sequence 3. Complete validation returned Clean and the query returned the
expected aggregate. Its normal retention fences permitted pruning sequence 1.

The first offline prune returned storage unavailable. One subsequent status
inspection, without retrying the prune, reported watermark 1 and one tombstone.
The mutation had committed. The function still owned the projection-sidecar
engine when its final status read tried to open that engine again.

Releasing that owned handle before the existing primary close and final status
read fixes the focused regression. It preserves deletion fences, sub-range
atomicity, the tombstone chain, error classes and all durable formats. The
permanent regression tests the first result and the independently committed
watermark; it does not retry the mutation.

## Projection recovery after a permitted prune

With the offline result fixed, the same genuine fixture reaches a separate
failure. The published aggregate and frontier remain exact after pruning, but
complete generation recovery returns projection-state integrity failure. Its
first authoritative scan requests commit 1, which storage correctly refuses as
HistoryPruned. Both retained-generation replay and initial generation building
start at the beginning of history.

The frozen campaign exercises this path. Backup restore moves the old data and
projection directories aside and uses empty replacement directories. Its public
restore invocation requests no sidecar preservation. The default restore removes
derived state under ADR-0246. A successfully restored authoritative database can
therefore accept writes while its aggregate projection lacks the history needed
to rebuild. An ordinary restart with a retained sidecar also hits the missing
prefix during generation validation; copying a sidecar alone does not solve it.

The 60-second first-write gate does not prove projection recovery. The campaign
separately requires projection-frontier progress and a final exact Ready query
at the current head. Those checks remain required.

## Architecture decision required

ADR-0086 qualifies rebuildability: history-dependent projections require their
complete input closure to be retained or backup-authoritative, or an explicitly
bound authoritative external source. Current ticket state is not a declared
rebuild source for the historical update-count projection. The retained source
and default backup composition currently do not close that dependency.

The concrete recovery design must preserve an authoritative replay source or
explicitly establish a backup-authoritative materialization with its validation
and lifetime rules. Any new durable identity, backup encoding, trust basis or
read-validation rule requires the corresponding accepted decision before
implementation. The external outbox destination cannot silently become a new
rebuild authority, and an unvalidated sidecar cannot certify its own contents.

Holding all history indefinitely, skipping a missing prefix, weakening the final
projection query or changing the frozen workload would conceal the limitation.
They are not milestone qualification. Quadratic prefix replay and changing-head
validation also warrant separate bounded-work and concurrency checks; neither
can recreate deleted input.

## Checks and remaining work

The offline defect has a genuine failing first-result regression and a passing
ownership correction. A separate native three-command/prune-one regression
reproduces the projection recovery failure after proving the original generation
Clean and its aggregate exact. The unresolved projection assertion remains
private investigation evidence, outside the passing implementation test suite.

The permanent offline correction also passes two server archive/sidecar tests,
ten storage-recovery cases covering offline fences and crashes, and the native
protected-reader engine-exclusion test. Both affected crates pass scoped Clippy,
and repository path/size guards pass.

These findings do not establish the cause of the earlier online retention
CorruptData refusal. That refusal remains open. Full canonical preparation,
full-size public recovery, full source CI, the eight-hour rehearsal and the
24-hour campaign are still required; this note claims none of those outcomes.
