# WP-825 physical provider ownership repair

The full-population diagnostic admits two distinct provider snapshots and
refuses the third, although their native physical union is about 350 MiB.
Distinct immutable generations currently charge their shared tree, key and
manifest ownership again. A canonical prune regression reproduces the same
refusal with 64 KiB of legitimate incremental provider headroom.

A second regression proves duplicate engine-cache charging across separately
admitted sources. The review also identifies archive caches configured at
64 MiB but estimated as 12 MiB, and derived reads retaining an engine different
from their initially empty transaction. Neither regression is a repair pass.

Both focused regressions now pass with the candidate repair. Repeating the
original full-population test admits 29 provider snapshots and refuses the
30th. Component attribution confirms repeated shared overlay and journal-suffix
charges of 22,468,312 and 25,281,066 bytes at that refusal. The original
32-snapshot assertion was still unsatisfied at that point. After repairing
actual shared source, overlay and metadata ownership, the same saved population passes
all 32 provider admissions and 32 retention publications. Original retained
outcomes and provenance remain exact; final provider ownership, reservations
and allocation counts all return to zero. Provider ownership reaches
423,447,479 bytes; native total and ordinary peaks are 403,569,440 and
393,409,184 bytes. Final native generation owners and retirement queues also
return to zero. These diagnostic results preserve the original fixture and
key files, under unchanged ceilings; campaign qualification remains open.

## Authority and scope

The maintainer authorized expanding scope to cover any necessary native fixes.
This registration applies that standing authorization to the concrete ownership
repair, under accepted ADR-0266 Decisions 3, 4 and 8 and ADR-0314 Decisions 2–6
and 9. No new guarantee, ceiling, durable format, fixture, authorization rule,
transaction order, recovery gate or application bypass is proposed.

WP-825 adds OQ-040 and these source paths for the numerical ownership primitive:

- `crates/riffdb-types/src/exact_provider_budget.rs`
- `crates/riffdb-types/src/exact_provider_physical_ownership.rs`
- `crates/riffdb-types/src/exact_provider_physical_ownership_tests.rs`

The existing storage-redb source scope covers shared engine custody, native
allocation ownership and focused tests. This registration does not authorize
the separate initial-admission audit exception proposed in ADR-0315.

## Implementation boundary

Use one admitted physical ownership ledger for each immutable sharing family.
An independent full reconstruction creates a family; successors preserve it.
The ledger charges the actual live family union once, conservatively including
its private staged successors. It excludes process-lifetime allocator backing
and unrelated independently reconstructed families. Native limits and quotas
remain process-wide. Cross-family node, key and manifest links must be checked;
current and checkpoint admission must cover their actual families together.

Capture stays constant-time. Admit live uncovered family bytes before publishing
provider ownership. Physical allocation destruction releases its exact numerical
fragment, including queued destruction; predecessor retirement never uncharges
still-shared allocations. Fixed family controls are admitted separately without
self-reference. Prepare every fallible increment and source/engine charge before
any final attachment or cut publication. Refusal leaves existing owners intact.

Each actual engine owns its configured cache charge until engine destruction.
Separate sources share that owner. An empty derived transaction retains its
exact lazy in-memory engine; later sidecar creation does not replace that read.

Overlay keys and values retain closed immutable byte owners. Each distinct
copied tree node has its own control owner; a uniquely owned private node keeps
its original one. Lineage allocations release after their last physical owner.
Size-derived metadata bounds replace the undercounted entry allowance while
preserving the 256-MiB overlay ceiling. This can produce an earlier typed
capacity refusal. Fixed construction scratch is checked before allocation.
The common family ledger and shared-owner cell pass 17 focused tests; the
overlay passes all 26 selected tests, including five ownership cases and existing atomicity,
private-path reuse, rebase, balancing and process-level lineage destruction.
Integrated native source, overlay, provider, engine, retirement and checkpoint
checks also pass. This remains focused validation, not campaign qualification.
History resolvers, composite views and cached root handles now carry ownership
on their actual shared allocations. Separately admitted sources reuse those
charges; each unique source capsule retains its existing 32-KiB allowance.
Tracked weak roots retain a charge-only control until the residual allocation
is released. Native construction and residual bounds include the actual
control and provider-token allocations without adding a generation owner slot.
Four genuine regressions fail before repair and pass afterward: same-source
incremental admission, resolver-only custody, cached-root custody and final weak
root release. Engine, provider-plan rollback, root tracking, capability and
checkpoint-pressure checks also pass. The complete repaired candidate passes
the saved population rerun described above. Scoped acceptance runs before the
implementation commit; complete source CI and campaign qualification remain open.

A separate checkpoint-pressure regression fails before the retry-state repair
and passes afterward. A capacity refusal during private rebase now retains the
completed I/O result and original admitted descriptor. Releasing pressure
publishes that same completed checkpoint once, preserving old and current audit
rows and history. Publication and invariant failures continue to fence writes;
publication, durable-frontier advancement and sealer ordering are unchanged.

## Required verification

First rerun the two focused ownership regressions on the qualification host.
Then check cancellation, foreign graphs, simultaneous allocation/free/cut, full
plan refusal, exact empty-to-real derived capture and archive cache capacity.
Rerun the saved 32-generation published-retention diagnostic before broad CI.
The frozen workload effect remains unmeasured; no rehearsal is qualified here.

The affected operational handbook page must describe the corrected physical
ownership. Existing gates, full preparation, CI and both rehearsals remain open.

## Implementation PR note

Package: WP-825 (open). Tier: guarantee. Behavior: retain one provider charge
through actual shared tree, source-buffer, overlay, engine, resolver, view and
root allocation lifetimes, including queued and weak-reference retirement.
Capacity refusal preserves completed checkpoint I/O and its original descriptor
for a later publication attempt. The complete incremental ownership plan stays
private until all fallible preparation succeeds, under accepted ADR-0266 and
ADR-0314.

Checks run on the qualification host: 17 common accounting tests, 26 overlay
checks, focused native ownership, provider rollback, engine, capability,
tracking and checkpoint-pressure tests, the genuine 32-provider/32-retention
population probe, scoped acceptance with tests excluded, requirement coverage
and ADR obligations. Scoped acceptance includes all-target/all-feature Clippy,
workspace formatting, scope, handbook, version topology and source guards.
Compatibility: durable encodings, fixtures, command outcomes, authorization,
ordering and compiled ceilings are unchanged. Corrected overlay metadata bounds
can refuse capacity earlier. Hazards and follow-ups: public restored-source
initial admission still requires the separate ADR-0315 decision; the accepted
ADR-0313 form correction remains under review. Complete source CI, full workload
preparation and both rehearsals remain unqualified. Updated handbook:
[Startup Integrity](../operations/STARTUP-INTEGRITY.md) and
[Sustainable TicketDesk](SUSTAINABLE-TICKETDESK.md).
