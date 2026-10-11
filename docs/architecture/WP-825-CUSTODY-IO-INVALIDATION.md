# WP-825 native custody I/O invalidation

Package: WP-825 (open). Tier: guarantee, implementing accepted ADR-0285.

The private custody foundation already retained sticky cancellation, identity
and lease-poison refusals. A native backend I/O failure did not invalidate its
owner, allowing engine close and another lease on the same inode and length.
Record failures from descriptor/backend construction, length/read/write/resize,
synchronization and close. The failed owner refuses every later lease even if
the underlying file becomes readable. Close still releases its engine lease,
and the outer native guard still excludes foreign owners until all handles drop.

Admission refusals before I/O and access through an already closed generation
remain distinct: they do not invalidate a healthy successor merely because an
old handle was used. No native I/O error is converted to success or absence.

Checks required on the required 8-vCPU, 32-GiB Linux SSD host: a genuine FileBackend failing-read counterexample;
all custody ownership, cross-process, cancellation and deterministic lease
schedule tests; native Clippy, formatting and allowed paths. Verify the actual
terminal receipt and exact formatted source before committing.

Compatibility: the foundation remains test-only until the complete verified
materialization factory integrates it. No public interface, durable format,
dependency, ordering, runtime workload or resource limit changes. No handbook
update is needed for this internal foundation correction.

Hazards and follow-ups: this is no proof that production has continuous custody.
The complete private factory, owned logical/physical proof, authorization reader,
final combined CI and unchanged recovery/rehearsal gates remain open.

Private preparation and verification have distinct length authority. Native
hydration may grow an unpublished copy under the existing allocator and caller
bounds. Only after the engine lease has actually closed can its exact completed
length become the verifier's capacity. A live engine cannot freeze that bound,
a repeated freeze cannot increase it, and later verifier resize or write requests
cannot extend the captured artifact. This foundation remains test-only: raw-path
capture cannot become a production constructor. Production construction must
consume complete authenticated copied-artifact provenance. A controlled stamp
must also retain its existing legitimate allocation behavior under its separate
receipt and pre-stamp seals; a frozen verification cap cannot silently narrow it.

Required native proof: fill a real owned engine beyond its initial file length,
observe close, freeze the exact resulting length, reopen and read every row,
then refuse extension. Verify independent-process exclusion before and after
the freeze and ordinary opens after final release. This adds no publication
authority or production custody integration.
