# WP-825 native restore owner review

Status: ADR-0293 was accepted exactly as written in session on 2026-10-12.
The single-engine stamp/close foundation is implemented and focused checks pass.
Complete-history proof reuse into stamping is implemented; watermark reuse and
delayed HISTORY hydration remain pending.
ADR-0290, ADR-0291 and ADR-0292
remain accepted. WP-825 and WP-826 remain open.

The approved restore work is checkpointed. The ordinary daemon reaches restored
readiness in 75.815435474 seconds on the preserved populated fixture; the
faithful primary-loss 60-second gate still fails during restore. Ninety-eight
focused native regressions, the ordinary release daemon build, allowed paths
and whitespace pass on the required host. Full acceptance, final-source CI,
actual scheduled-fault qualification, eight-hour rehearsal and 24-hour campaign
remain pending. No old failure has been revised into a success.

## First implementation checkpoint

The ordinary controller now retains a unique live preparation owner until its
receipt authorizes the existing incarnation stamp. It consumes all authorization
readers, verifies full artifact content, stamps on that same engine and observes
its one close before minting the final publication seal. A private live/closed/
invalidated capability distinction refuses publication before stamping and
prevents retrying a failed transition through the legacy branch. Direct closed
stage, archive and promotion workflows preserve their previous close protocols.

Three new lifecycle tests, 31 existing custody/compatibility tests (including
those three), one real publication test and six maintenance polling tests pass
on the required host. The release daemon build uses its ordinary profile. The
stronger primary-loss whole gate still fails during restore; a subsequent
restore-only diagnostic measures 72.304798916 seconds. That diagnostic proves
neither the whole gate nor campaign qualification. Complete transaction-current
history validation still runs here; proof reuse and discarded-receipt hydration
avoidance have not been implemented by this checkpoint. Full acceptance, final
CI, crash-matrix verification and both campaigns remain pending.

## Complete-history proof checkpoint

A private constructor now performs the entire original retained-history check
and binds its result to the checked native engine, original custody and mutation
generation. Bounded ordinary readiness carries that evidence separately from
its assurance and immutable reader. Exact materialized-journal inspection stays
unchanged. After all readers are released, the stamp verifies its current bounded
roots and binding, retains the existing admission/lifecycle preflight, and
consumes the evidence before mutation. The new canonical RestoreAnchor still
receives complete validation. Legacy complete and archive construction retain
their original protocols.

Three constructor/ownership tests, three readiness tests, 34 custody and
compatibility tests, 12 stamp compatibility tests and the real publication
test pass on the required host, alongside the ordinary release daemon build.
The tests reject valid terminal roots with a corrupt middle receipt, an
unclassified native commit and rebinding to another owner. The stamp count
asserts that only the new anchor needs another full walk. The faithful whole
60-second gate still fails during restore; the component attribution is pending.
This checkpoint does not implement watermark proof reuse, avoid predecessor
HISTORY serialization, qualify final CI or start either campaign.

## Measured remaining work

A private diagnostic build of the same native implementation adds only static
phase names and monotonic timings. It measures a 75.490149914-second restore.
Its probes have been removed and the production source independently rechecked;
the instrumented binary is not a qualifying artifact. Nested phase durations
must not be added to their enclosing phases.

| Phase | Seconds | Included work |
|---|---:|---|
| Initial immutable validation | 1.0 | Full backup primary/journal checks and manifest binding |
| Materialize and validate stage | 51.7 | The following preparation and readiness work |
| Copy/verify before hydration | about 17.2 | Primary, journal, sealed extents and their original checks |
| Hydrate history | 14.2 | Resolve and durably materialize COMMITS and HISTORY |
| Watermark preparation | 9.0 | Complete retained-history and retention fencing work |
| Prepare/check native engine | 3.2 | Compatibility, journal recovery and nonrepairing physical check |
| Materialized-journal/history check | 3.9 | Complete same-state retained history and exact journal materialization |
| Initial content seal | 3.2 | Primary, journal and marker hashing |
| Authorization-owner close/seal | 5.5 | Pre-close full seal verification, close and successor sealing |
| Receipt-authorized stamp | 9.7 | Includes 6.1 seconds in transaction preflight, reset and commit |
| Publication | 8.5 | Revalidate immutable backup, copy and independent destination readback |

The source has an additional lifecycle redundancy: `hydrate_history_stubs`
materializes predecessor HISTORY receipts before `PreparedRestoreAnchor::stage`
deletes HISTORY and creates its single successor anchor. The predecessor chain
still needs complete validation. Its temporary serialization can be avoided by
retaining the verified source extents until the stamp removes those references.
Surviving command values must remain self-contained and unchanged.

## Concrete proposal

[ADR-0293](../../adr/0293-native-restore-uses-one-validated-preparation-owner.md)
changes three internal lifecycle boundaries:

1. Reuse a typed complete history-validation proof within one continuously owned
   state. Each use checks transaction-current roots and native custody. Existing
   chain, source-hold, registration, admission, cutover, fencing and journal
   checks still run before the proof can exist. Known validated receipt advances
   can establish an exact successor; unknown changes invalidate it.
2. Retain copied source extents until every surviving value is self-contained.
   Validate the predecessor chain through those extents, then let the existing
   atomic RestoreAnchor discard predecessor HISTORY without serializing it just
   to delete it. Extent changes fail closed and never become a new trusted seal.
3. Release every authorization reader and retain the sole private engine through
   the receipt-authorized stamp and its actual close. Fully verify the seal
   before that combined controlled span and establish the final full seal after
   it. An open owner cannot enter publication.

The proposal preserves final artifact verification, the second publication copy,
its independent destination readback, retained-stage crash reconciliation,
current receipts, durable encodings and existing public interfaces. It changes
no workload, resource ceiling, gate, assurance claim or acknowledgement policy.
No timing estimate is claimed as a passing result.

## Implementation shape and review checks

- Introduce an internal live stamp-preparation owner distinct from the closed
  publication owner. The staged seal holds the live variant until the existing
  receipt choice is durable and stamping consumes it into the closed variant.
  Publication rejects the live variant. Archive/follower paths retain their
  current owners; no application-facing selector is added.
- Capture complete same-engine history evidence after the required recovery and
  before its first use. Keep its typed semantic binding separate from content
  seal continuity. Reuse current semantic types; do not duplicate history roots
  or treat a decoded checksum as full validation. Any genuine replay or
  compatibility mutation requires an exact successor or complete revalidation.
- Keep hydration batches and decoded-frame admission bounded. Hydrate surviving
  COMMITS values, keep original HISTORY stub sources through complete validation
  and authorization, and retire extents only after the canonical stamp leaves
  no surviving references. Retain the existing final journal generation fencing.
- Deterministically retain/release readers, mutate same-length source files and
  middle history rows, and exercise no-op and real watermark transitions. Cover
  process crashes at receipt choice, stamp, close, source retirement, sealing,
  publication and dirty consumption. Exactly-once incarnation and original
  replay/provenance/state reconciliation remain mandatory.
- Rerun the saved failing public gate immediately after each implementation
  change. Only after it passes run required crash/compatibility checks, scoped
  acceptance, full CI and the actual scheduled-fault population qualification.

AGENTS.md requires exact human acceptance of guarantee-tier records before
merge. This proposal amends ADR-0290's separate mutation/close seals and
ADR-0291's staging lifecycle. The earlier broad native-fix approval covers the
implementation paths. The maintainer separately accepted this exact record;
acceptance does not qualify its implementation or any timing result. The
previously accepted clock/controller work remains authorized and independent.
