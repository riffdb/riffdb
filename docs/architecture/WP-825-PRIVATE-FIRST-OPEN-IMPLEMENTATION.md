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
