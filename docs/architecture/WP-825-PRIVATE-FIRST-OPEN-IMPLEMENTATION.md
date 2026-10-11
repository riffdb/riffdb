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
