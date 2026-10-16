# WP-825 native directory capacity and retention follow-up

Package: WP-825. Tier: guarantee, within accepted ADR-0314 and ADR-0315.
This is an implementation review and PR note, not a new capacity policy.

## Behavior changed

A healthy unfinished native command directory now yields an explicit online
retention hold. The worker releases its writer lease and uses its existing
cancellable backoff. It checks the current source again under the lease before
admitting a prune. Invalid directories, fencing and actual storage failures
retain terminal refusal. Pending does not count as successful retention progress
and does not restart construction.

Each charged native allocation now stores its value once, in the original
precreated retirement task. Final reference release enqueues that same task.
Destruction, physical family charges, provider ownership and generation quotas
remain deferred through the existing bounded reaper and causal epochs.
The aggregate 512-MiB, ordinary 448-MiB and 1,024-owner ceilings are unchanged.

## Evidence and checks

The full-size preparation stopped with storage unavailable during ordinary
load. A new copy of its failed media independently reproduced storage unavailable
on the first authenticated health request. A diagnostic build identified an
ordinary native allocation refusal: 453,003,666 bytes were charged when directory
construction requested its existing 16,777,472-byte decoding workspace, exceeding
the 469,762,048-byte ordinary ceiling. This establishes a cold construction
capacity limitation; it does not prove the original live failure had only this
cause. Original failure media is preserved outside the repository.

A deterministic native test reproduced healthy Pending as a terminal retention
error while preserving the original watermark. Completing the original owner
then allowed the same genuine eligible command to be pruned. The repaired test
checks the explicit hold, source changes before writer admission, corruption,
fencing, cancellation and no false progress.

A second deterministic regression used one real non-Clone 64-KiB value with
96 KiB of admitted headroom. The old duplicated representation refused before
construction; the single-value representation passes and verifies alias contents,
deferred physical destruction, provider/quota lifetime and cancellation cleanup.
The focused run also passed 99 native ownership/source tests, two server tests
and 59 architecture checks. Scoped acceptance, including formatting, clippy, handbook, path and panic
checks, also passes. Production-size confirmation remains outstanding; no
campaign qualification is claimed here.

## Compatibility and operator documentation

No durable format, registry identity, authorization rule, public protocol,
application setting, transaction ordering or acknowledgement changes. The
[operational startup page](../operations/STARTUP-INTEGRITY.md) explains the
retention hold. The test-fixture lint correction names two ownership-only fields explicitly;
their destructor order and assertions remain.

## Hazards and follow-ups

Current event-consumer checkpoints advance with acknowledged selected events.
Healthy empty pulls for a quiet partition preserve the global history fence
(SPEC CON-003 and ADR-0085). This can protect unrelated history indefinitely;
the frozen workload can legitimately retain roughly one 65,536-update cycle
between events for its selected partition. Its 32,768-sequence retention target
cannot override that fence. This is an architectural constraint, not stalled-ACK
proof or permission to prune protected history.

Safe progress across proven irrelevant history would require jointly reviewed
consumer continuation and durable scanned-through evidence. A collector-only
filter or an invented checkpoint at an unrelated event would break current
recovery semantics. That decision remains separate from these repairs.

Production-size native construction, fresh full preparation, public restore,
full source CI, the eight-hour rehearsal and the 24-hour campaign remain required.
