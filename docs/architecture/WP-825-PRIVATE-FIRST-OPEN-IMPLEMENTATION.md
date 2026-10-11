# WP-825 private materialization first-open repair refusal

Package: WP-825 (open). Tier: guarantee, covered by accepted ADR-0289.

The first private restore copy used an ordinary recovering engine open before
the later repair-refusing hydration and complete verification. A repair at that
first boundary could therefore disappear behind a subsequent clean open.

Private materialization now authenticates the copied primary, journal, marker
and sealed extents before opening the private primary with repair refusal.
The existing exact manifest-semantic comparison still runs on the opened
database. Ordinary offline restore retains its existing recovery behavior and
mutation order. This correction adds no application selector or durable format.

The regression constructs a real native backup, makes its primary require
allocator repair, and authenticates those bytes in its manifest. Private
materialization must return `CorruptData`, leave every backup artifact intact,
and publish neither a staged primary nor the configured target. The same test
must fail against the preceding implementation before the correction is tested.

Checks run on the required 8-vCPU, 32-GiB Linux SSD host: the failing first-open regression, the corrected regression,
native backup and maintenance tests, native Clippy, formatting, allowed paths,
and scoped acceptance. Review the bound receipt before committing this change.

Handbook: `docs/backup-restore.md` documents first-open repair refusal.

Compatibility: original manifest comparisons, journal and marker predicates,
history hydration, retention-watermark preparation and target publication remain
required. The existing offline restore path and compatibility formats remain.

Hazards and follow-ups: this entry-point correction does not provide ADR-0289's
continuous engine custody or retained complete native validation proof. Those
remain implementation work. The native retention refusal, final combined-source
CI, unchanged whole-action recovery measurement and real rehearsal are also
open; no campaign qualification or package closure is claimed here.


The next native materialization boundary retains the three creation descriptors
and their directory capabilities before copying. Existing checksum checks read
those retained private files. The first private engine is opened through the
original primary descriptor, and every known staging rename rechecks the same
file and directory identities. Only the existing no-journal compatibility path
may create its known replacement journal before any proof or engine exists.
Ordinary offline restore retains its existing engine and transaction path.
Sealed extents keep their descriptor-free staging vector; this adds a fixed
three artifact pins rather than one open descriptor per possible extent.

Required native counterexamples replace primary, journal, marker or parent with
identical bytes before first open. All must refuse before engine writes; healthy
manifest facts, known renames, genuine backup compatibility, crash cuts, repair
refusal and immutable originals must remain valid. This boundary remains short
of the complete ADR-0289 owner: it does not retain outer native custody through
full validation, authorization, stamping and publication, or remove the second
verification copy. Final combined CI and unchanged public gates remain open.


A pending native factory can consume the actual verified copied-primary
creation descriptor and directory capability into continuous exclusive custody
before the first engine open. It keeps that first engine through the existing
hydration and watermark preparation, performs its known rename under the same
owner, and retains custody after observed engine close. Raw-path custody
construction is test-only. Its three fixed artifact pins add no per-extent
file-descriptor population. Cancellation remains sticky after an observed reset.

This factory is not yet the production stage constructor. Complete owned
physical/logical validation, compatibility preparation including legacy journal
identity transitions, the unchanged server proof join, authorization, controlled
stamp and publication still require integration. The existing full-stage
custody counterexample remains open. A successful materialization-owner test
cannot qualify that counterexample or the whole public recovery action.


A native counterexample also shows that redb can close its backend while a
`ReadTransaction` remains alive. Native backend-close observation therefore
cannot prove that read pins have been released. Preparation reads retain the
same `Database` owner explicitly; a second pin and a close with an outstanding
pin refuse without consuming that owner. The original read must remain usable,
and only its release permits close and the exact-length freeze. The final
logical authorization reader has the same obligation through its retained
shared engine owner. This correction changes the native ownership protocol,
not the harness or the accepted release condition.

The next internal handoff carries the first private engine, its actual checkpoint
rewrite observer and repair latch into the existing native store join. It runs
physical integrity before admitting a read pin, verifies the native journals,
and invokes the existing complete structural and catalog validators through both
exact ends on that same engine. Private validation allocates no preparation
workers and remains write fenced. This internal result grants no publication or
serving authority. Required compatibility preparation, ordinary server joins,
closed-engine seals and production staged-restore integration remain unfinished;
component checks cannot qualify the campaign.

The owned backend and existing ordinary opens converge on the same redb builder;
the fixed cache and repair callback remain centralized. The complete logical
proof is crate-private to permit the native backup adapter to retain it, and is
still absent from the public crate exports and operational-port interfaces.

The preparation experiment invokes the existing format, locator, journal-recovery
and pending-registry routines in their original order before admitting the
complete validation pin. Unique ownership permits the physical integrity pass
after these writes on the same engine; the complete journal, structural and
catalog checks then observe the prepared frontier. A test removes the actual
three derived locator tables before producing an immutable backup and requires
the native owner to restore them without changing that backup. Post-validation
registry completion and legacy journal identity transitions still need their
native handoffs before production integration is complete.

A native proof-binding experiment attaches the original backend's file, lease
and mutation generation to the same engine container and private store. Every
backend write or resize advances a checked generation, including raw engine
commits which bypass the store commit counter. Only the actual complete physical
pass on the uniquely owned engine records a validated generation. Logical proof
completion and cached authorization-reader clones recheck that binding and the
original primary identity; observed uncertainty permanently refuses reuse.
Ordinary source and follower construction carry no restore binding. This still
does not implement the server join, sidecar sealing or publication lifecycle.

Every semantic owned-snapshot reader checks an attached completed-validation
custody before accessing cached rows or allocating provider input. Ordinary
operational snapshots have no such attached private proof. Native regressions
exercise the actual frontier, catalog and capability-reader interfaces after
uncertain primary identity or an untracked engine commit, including cloned reads.

The compatibility handoff consumes the original complete read before the native
compatibility transaction, uniquely retains its engine, and retires the old
physical binding before that known write. It reuses the original first V3
activation transaction or the existing atomic registry/checkpoint/receipt
transaction as applicable. Ordinary activation retains its checkpoint, DIRTY and
readiness sequence. The private handoff repeats physical, journal and both
complete evidence streams with the same value-only startup inputs, without
operational admission. A second required transition is refused. A clean physical
recheck cannot reset a binding after an unclassified engine write. The server
join, final close/seals and production restore integration remain outstanding.

The authorization handoff binds all three artifacts through their original file
descriptors before handing out the immutable reader. Changed bytes on the same
inode invalidate the native owner before another seal can be minted. The close
handoff requires every actual reader clone to have released the first engine,
checks the old byte binding and native mutation binding, observes actual engine
close under outer exclusion, and only then binds the closed checksums. Engine
close may change its own clean header and allocator metadata; the journal and
marker must retain their prior checksums. This internal seam still requires the
server's unchanged startup proof join, receipted stamp and production wiring.

The closed owner can apply the existing incarnation stamp through a fresh
repair-refusing native sublease of the same retained inode. It verifies all old
seals before retiring them, permits the native allocator's required growth, and
reuses the original transaction-current history validation, restore-anchor and
replication reset inside the same Immediate two-phase transaction. Actual engine
close precedes the new exact-length freeze and checksums. The journal and marker
retain their old seals, and any transition error permanently invalidates the
owner. Equal-incarnation retry retains the original no-write behavior. Receipt
authority, server proof joins and publication still belong to their existing
composition; this internal seam does not itself authorize target publication.

Promoted backups use their existing private promotion binding, derived from the
actual owned engine and the verified immutable manifest. This preserves the
original complete private validation and skips source-only preparation and
registry writers. The copied promotion record cannot reconcile the configured
source's external authority, and no source port is created. Physical verification,
immutable authorization, reader-release checks and closed seals retain the same
native owner. Ordinary promotion reconciliation and receiver construction remain
unchanged. Production stage wiring remains required.

The cross-crate evidence bridge has no public constructor. Only the actual native
materialization proof can construct it; it retains the exact value-only startup
inputs and immutable reader. The server adapter checks the current configured
input binding and calls the same `validate_ready_join` used by ordinary startup,
including catalog/session, retained identity, active catalog, bootstrap lifecycle
and both allocator-capacity checks. It returns only an immutable authorization
reader and never invokes port activation, publication seeding or workers. The
production driver has not yet switched to this internal adapter; its existing
whole-stage custody regression remains an open integration gate.

The native source-compatible owner consumes an actual catalog migration context
only after both complete streams reach their exact ends with zero findings.
It drops every old immutable pin before retiring that proof, and invokes the
existing catalog-owned bounded index migration driver and native backend with
their original hardened transaction grouping. Private promotion receives no
migration writer. Every compatibility write is followed by fresh physical,
journal, structural and catalog validation. The closed sequence admits at most
one index migration and one registry transition, hence at most three complete
passes for the oldest supported input; repeated transitions fail closed. Public
offline scrub keeps its original refusal behavior and ordinary startup is
unchanged. Production stage wiring and final combined-source CI remain open.

The equal-incarnation test compares the owned retry to the actual unchanged
original stamp routine on an exact copy of the first sealed primary. The
original abort/open/close itself changes native housekeeping bytes; byte
identity across that known transition is not its idempotence contract. Both
paths preserve identical database/catalog/frontier facts, retained allocators,
V3 history and every encoded receipt. The owned path also matches the original
post-close primary bytes exactly and preserves journal and marker bytes. The
original failed byte-identical assertion is retained as failure evidence. This
test correction changes no runtime stamp transaction or retry semantics.
