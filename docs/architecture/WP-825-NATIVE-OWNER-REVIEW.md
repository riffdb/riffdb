# WP-825 native restore owner review

Status: ADR-0293 was accepted exactly as written in session on 2026-10-12.
The single-engine stamp/close foundation is implemented and focused checks pass.
Complete-history proof reuse through watermark preparation and stamping, plus
delayed predecessor HISTORY hydration, are implemented. The latest focused
native checks and ordinary build pass. The faithful whole gate restores the
database in 57.949224399 seconds and completes its first write at
59.867267944 seconds. It still fails during original-identity replay, before
full-state reconciliation.
ADR-0290, ADR-0291 and ADR-0292
remain accepted. WP-825 and WP-826 remain open.

The approved restore work is checkpointed. The last measured ordinary daemon
completes restore in 57.949224399 seconds on the preserved populated fixture.
The faithful primary-loss 60-second gate still fails during original-identity
replay, after its first successful write. Focused native regressions, the ordinary release daemon build,
allowed paths and whitespace pass on the required host. Full acceptance, final-source CI,
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
60-second gate still fails during restore; a subsequent restore-only diagnostic measures 68.191618524 seconds.
This checkpoint does not implement watermark proof reuse, avoid predecessor
HISTORY serialization, qualify final CI or start either campaign.

## Deferred source lifecycle checkpoint

The ordinary server's receipt-authorized constructors retain the copied sealed
extents and their original creation descriptors. They hydrate surviving command
values, then perform complete predecessor-history validation after compatibility,
journal recovery and physical checking. Equal watermark retries and bounded
readiness reuse that same-state evidence. Original fencing and tombstone checks
remain; a real watermark transition retires the proof, uses the original full
transaction and requires a fresh physical check and complete history proof.

The stamp consumes all readers, atomically replaces predecessor HISTORY with its
canonical RestoreAnchor, checks for any surviving command or receipt stub, and
observes engine close. Only then can the controlled span retire the checked
original extents and renew and completely verify the empty journal above the
target generation. A private verified journal-successor binding permits exactly
that change before final sealing. Legacy direct close refuses a deferred stage;
ordinary direct offline, archive, follower and promotion paths retain their
previous protocols. Unsupported images use complete preparation.

Six readiness tests, 34 existing native custody/compatibility tests, 12 stamp
compatibility tests, one real publication test and six maintenance tests pass.
They verify the single complete constructor, source lifetime and refusal of
missing, replaced or same-length modified extents. The ordinary default-profile build passes. The faithful
populated whole gate still fails during restore; subsequent restore-only
attribution measures 64.148945502 seconds. Independent checks bind the actual
source, logs, frozen binaries and unchanged original backup and fixture. No
failure is converted into a pass. Further process crash
coverage, final acceptance/CI and both actual campaigns remain pending.

## Fencing proof follow-through

A subsequent private profile confirms that the deferred lifecycle is selected.
Watermark preparation takes 27.428 seconds, including 13.155 seconds for its
complete retained-history constructor. The restored follower-fence collector
then invokes the same full validator again. The profiled restore takes 63.769
seconds; instrumentation is diagnostic only and production bytes are restored
and independently verified afterward.

A focused counter test reproduces the extra full walk before the fix. The native
restore fence collector now accepts only the same-state complete proof, checks
its current read pin and native generation before and after collection, and
retains all projection, operator, outbox, migration, consumer and follower minima.
Every source hold is still decoded; the constructor already checked its exact
key, lineage, ancestry and registration links. Other offline and online collectors
retain their original full-validation behavior.

The focused test compares the complete fence result against the original
collector with a genuine follower hold, proves no extra full chain walk, and
refuses reuse after a native hold mutation. This test, existing retention,
readiness, custody, stamp and publication tests pass, including nine retention
tests, six readiness tests, 35 custody tests and 12 stamp tests. The ordinary
default-profile release build passes. The faithful populated gate completes
restore at 58.213811086 seconds after primary loss, then commits its first write
but records completion at 60.000607091 seconds. This exceeds the unchanged
60-second deadline. The exact retries, original acknowledged-identity replay and
full 65,536-ticket reconciliation do not run. Backup preparation took
16.348319239 seconds before primary loss, with no intervening acknowledgements;
it is reported separately. Independent verification binds the actual source,
logs, frozen binaries and unchanged original inputs. This remains a failed gate;
neither campaign has started.

## Native receipt decoding follow-through

Full retained-chain validation resolves a stub by completely decoding its journal
frame and constructing the checked native net receipt. It previously serialized
that receipt, hashed the generated bytes, decoded those bytes into a second
receipt and hashed them again before validating the chain. The new private path
uses the original complete journal decoder and receipt constructor directly.
Inline durable receipts keep their full canonical decoder. Receipt ordering,
namespace, allocator, attribution, binding, size and lineage checks remain;
chain advancement still computes the canonical receipt hash. The byte-returning
resolver and admitted provider readers retain their existing interfaces.

A genuine retained-journal test reproduces the serialization before the fix and
then checks typed and canonical byte equality, history advancement, malformed
inline content, wrong stub kind/hash and altered source content. The focused
receipt, cursor, stub, retention, readiness, custody, stamp and publication tests
pass on the required host, along with the ordinary default-profile release
build. The faithful whole gate completes restore at 57.949224399 seconds and
its first write at 59.867267944 seconds. The first-write exact retry passes;
the original acknowledged-identity replay exceeds the same 60-second deadline.
Full state reconciliation does not run. This small elapsed improvement does
not qualify recovery or either campaign. Independent verification binds the
actual source, logs, binaries and unchanged original inputs.

## Earlier attribution motivating ADR-0293

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
