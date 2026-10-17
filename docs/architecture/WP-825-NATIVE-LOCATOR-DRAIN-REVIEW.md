# Native locator construction at graceful close

Package: WP-825. Tier: guarantee, within accepted ADR-0157 and ADR-0314.
The startup handbook describes the operational change in
[Startup integrity](../operations/STARTUP-INTEGRITY.md).

A locator builder can outlive the shared storage handle while its captured
checkpoint still owns the actual redb engine. An explicit close could write
CLEAN before that builder released its source, leaving the file unavailable
for an immediate exclusive reopen. Counting shared-port references did not
prove physical engine release.

Each actual engine now owns an admitted construction domain. A queued or active
job registers with that domain; replacement source roots use the same engine
domain. Closing stops new construction admission, cancels only that domain's
queued jobs, and waits for active jobs to release their real sources. Queued
extraction preserves other databases' FIFO order and drops sources outside the
global queue mutex. Job completion releases source custody, the active build
predicate, and then the registration acknowledgment, in that order.

The deadline is one second from the original close entry. Drain happens before
acquiring the existing mutation lease, so a constructor already waiting to
publish can finish cancellation. Under the lease, close rechecks the closed
domain proof and retains the existing barrier, checkpoint classification and
Immediate CLEAN write. Drain timeout, drain refusal or an invalid drain proof leaves the barrier
failed and CLEAN unattempted. A successful existing verified DIRTY activation
may rearm a fully drained domain; an old close proof cannot authorize its new
epoch. Follower, private-validation and fenced-source refusals happen before
construction admission is closed.

Domain control has separate native and provider ownership charges, including
when a registration outlives the engine. Completed protected source captures
remain readable and charged until their final owners release. Cancellation
leaves a partial directory Pending; it never manufactures negative membership
or completion. No destructor waits for a global worker or performs a global
shutdown.

Focused native checks include an actual populated builder blocked on the mutation
lease while close drains outside that lease, final CLEAN with zero construction
registrations, unchanged checkpoint bytes, final engine release and one immediate
exclusive reopen. Other schedules prove that a target closes while another
database's active worker stays paused, and that a caught construction panic
releases actual engine custody before acknowledgment. Original close controls,
all three preserved route-corruption controls, and genuine protected positive
and negative command lookups pass on redb 4.4. Native all-target, all-feature
Clippy and default-feature Clippy pass.

The failure evidence remains distinct: the original deterministic schedule
reached final CLEAN while its builder was still active. Two later test drafts
blocked on their own observation of a mutex held by a paused close; moving that
observation before close fixed the schedule without changing production locking.
A subsequent run proved drain, CLEAN and engine release, then failed a separate
cold-open close assertion. That new source now completes its supported admitted
initial native dependency before its first close; dependency failures propagate,
and no close retry or fabricated completion is used.

Compatibility: no durable bytes, registry, dependency, application interface,
command ordering, acknowledgment or compiled ownership ceiling changes. The
existing 5-second administrative, 45-second release and 60-second recovery gates
remain. No compatibility fixture is changed by this repair.

Scoped acceptance subsequently passed all six required source and handbook
checks. Its receipt SHA-256 is
`32e60164fcedb4afdad4f12ce738d49c57da3863b3b25e0236865d7fd2816422`.

Hazards and follow-ups: final integrated CI remains required. These checks do not qualify a
shipping restore, the full frozen workload, either long rehearsal, the original
retention corruption investigation, or the projection replay-source lifecycle.
WP-825 and WP-826 remain open.
