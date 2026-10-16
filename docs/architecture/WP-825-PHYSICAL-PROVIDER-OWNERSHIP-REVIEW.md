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
32-snapshot assertion remains unsatisfied; shared source ownership is the next
native repair, under the same standing authorization and unchanged ceilings.

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

## Required verification

First rerun the two focused ownership regressions on the qualification host.
Then check cancellation, foreign graphs, simultaneous allocation/free/cut, full
plan refusal, exact empty-to-real derived capture and archive cache capacity.
Rerun the saved 32-generation published-retention diagnostic before broad CI.
The frozen workload effect remains unmeasured; no rehearsal is qualified here.

The affected operational handbook page must describe the corrected physical
ownership. Existing gates, full preparation, CI and both rehearsals remain open.
