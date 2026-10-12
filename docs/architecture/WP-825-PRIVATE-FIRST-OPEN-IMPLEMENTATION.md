# WP-825 continuously owned private restore validation

Package: WP-825 (open). Tier: guarantee, implementing accepted ADR-0289.
Handbook: `docs/backup-restore.md`.

Private restore now retains the actual native owner from authenticated backup
copy creation through complete validation, staged authentication, engine close,
receipt-authorized incarnation stamping and the existing target publication.
Ordinary restore, archived backup restore and source-less archive recovery use
this composition. The additional full restore-verification copy is removed.

## First open and native custody

The copied primary, journal, marker and sealed extents are authenticated before
the first private engine open. Exact manifest-semantic comparisons remain
required. Creation descriptors and directory capabilities retain the three
primary artifacts; known staging renames recheck their original identities.
The engine opens through the retained primary descriptor with repair refusal.
An independently owned outer native exclusive guard survives each backend close;
backend subleases cannot release or downgrade it. Raw-path custody construction
is test-only. Sealed extents do not add one descriptor per possible extent.

The owned backend and ordinary opens use the same existing redb builder, fixed
cache and repair callback. Preparation reads explicitly retain their `Database`
owner because a surviving `ReadTransaction` alone cannot prevent backend close.
Known preparation writes admit allocator growth under the existing resource
bounds. An exact captured-length freeze applies after observed engine close.

## Complete validation and compatibility

The actual private engine performs physical integrity before admitting a read
pin, then native journal, complete structural and complete catalog validation.
Both complete streams must reach their exact ends with zero findings. Private
validation allocates no preparation workers and grants no operational port or
serving authority. The crate-private result retains the actual engine, immutable
pin, catalog proof, original startup inputs and native validation binding.

Ordinary source compatibility uses the existing format, locator, journal,
registry and catalog-index preparation in its original transaction grouping.
Each required compatibility write consumes the old pin and physical binding,
uniquely retains the same engine, and repeats complete validation afterward.
The bounded transition sequence permits at most one index migration and one
registry transition, hence at most three complete passes for the oldest supported
input. Ordinary activation retains its checkpoint, DIRTY and readiness sequence.
Private promotion receives no source migration writer.

Archive image validation preserves the immutable backup's exact history fence.
It performs full source verification without appending ordinary source
compatibility receipts before archive replay. The retained archive preparation
owns the existing conversion transaction, checked empty-journal removal, original
follower proof join, selected replay and projection rebuilding. An interior cut
retires the old full-selection engine and readers, rematerializes the immutable
backup, and retains its typed predecessor, logical frontier, quarantined marker
and reciprocal command graph checks. Configured receiver recovery is unchanged.

## Authentication, close and publication

The opaque cross-crate bridge has no public constructor. The server checks the
exact startup-input binding and calls its original `validate_ready_join`, including
catalog/session, retained identity, active catalog, bootstrap lifecycle and both
allocator-capacity predicates. It receives only an immutable authorization reader;
fresh staged credential verification remains required before publication.

Every authorization reader clone retains the same native engine. Semantic reads
check the completed custody binding before accessing cached rows or allocating
provider input. Backend writes and resizes advance a checked mutation generation,
including engine commits outside the store commit counter. Unclassified writes,
artifact identity changes, observed I/O uncertainty or cancellation permanently
invalidate the owner; restoring bytes cannot revive its proof.

The primary, journal and marker byte bindings precede authorization. All actual
readers must release before close, and a close attempted with a surviving clone
refuses permanently. Only the observed known engine close may update the primary
seal for its clean header and allocator metadata. The sidecar seals remain intact.
The matching durable receipt authorizes the original Immediate two-phase
incarnation transaction, RestoreAnchor journal creation and marker restoration.
Known changes explicitly retire their old seals while original file identities
and native custody continue. Equal-incarnation retry preserves no-write behavior.

A fresh complete receipt-stamped archive source proof precedes the final closed
three-artifact seal. That proof grants no second authorization or activation.
Target exclusion, source/target revalidation, publication copies, marker-last
rename, uncertainty recovery and fresh post-publication startup are unchanged.

## Verification and remaining gates

Native counterexamples cover repair at first open, identical-byte file and
parent replacement, reader lifetime, unclassified commits, stale byte seals,
engine exclusion and the immutable archive history fence. Healthy backup
compatibility, complete proof joins, cancellation, genuine process crashes and
receipt retry assertions remain required on the specified 8-vCPU, 32-GiB Linux
SSD host. Native publication crash tests exercise both durability profiles.

The 18 native archive-stop cases and three publication-resume receipt phases run
as individually bounded tests. They retain the original case matrix, setup and
assertions, the existing CI watchdogs and the 60-second whole-action recovery gate.
No failed or interrupted acceptance run qualifies the candidate. Exact-source
scoped acceptance, full branch CI and ordinary frozen release binaries remain
necessary before fresh preparation and whole-action recovery measurements.

Compatibility: durable formats, authorization policy, public selectors, fixed
caches, transaction grouping, ordinary offline recovery and immutable backup
artifacts are preserved. Hazards and follow-ups: actual free-space admission,
measured recovery, the real eight-hour rehearsal, independent evidence checks,
package closure and merge remain open. No eight-hour or 24-hour qualification is
claimed by this implementation note.
