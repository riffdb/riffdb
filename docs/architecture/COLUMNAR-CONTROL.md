# Schema-Bound Columnar Control

RiffDB resolves each configured scalar projection name and each compiler-declared
vector projection through the active checked contract bundle at startup. The
name remains the application-facing lookup value; it is not a durable key and
never becomes a directory component. A scalar source is contract lineage plus
its existing physical definition fingerprint. A vector source retains contract
lineage, entity ID, and vector field ID.

The complete columnar specification separately binds primary-key order and
codec facts, all projected and organization-scope types, provider descriptors,
policy mode, replay limits, and vector production settings. A name-only change
therefore preserves both source and specification. A changed scalar physical
fingerprint creates a new source. Other semantic changes under the same source
allocate a disjoint rebuild and make the predecessor unservable.

Startup admits at most 256 distinct sources as one set. Empty, path-shaped,
duplicate names and duplicate aliases for one source are rejected before any
columnar control or projection file is touched. Every absent source receives a
durable `BeforeFirst` retention fence before command writers and projection
workers start. On first demand, the worker rebuilds V1 from one authoritative
snapshot plus the retained contiguous tail. Only a fully synced, reopened,
checksummed immutable manifest can advance the fence or publish.

The common durable control is the sole selector. V1 manifests live below a
schema-hash directory and use `MANIFEST-V1-<sha256>` names; opening requires the
exact length and checksum recorded by control. Directory enumeration, a legacy
`MANIFEST`, and name-derived directories cannot choose state. A crash before
the control update leaves an ignored orphan. A crash after it reopens the exact
selected artifact.

## Canonical vector cells in V2

The [accepted canonical-vector amendment](WP-747-V2-VECTOR-REVIEW.md) stores
checked `Vector` and `Optional<Vector>` cells losslessly in existing V2 Bytes
lanes. Generation validation restores the vector type and checks the declared
dimension and exact canonical bytes before comparing against authoritative
input. Null keeps its existing validity representation. Malformed payloads,
non-finite components, negative-zero encodings and mismatches refuse the whole
generation. Vector byte ordering and statistics provide no pruning evidence.
This changes no physical tag or scalar artifact bytes. Primary publication
still requires exact durable selection; followers use the separately accepted
disposable views without source-checksum claims or local control writes.

## V2 activation and immutable generations

Each database and source activates V2 independently. A selected V1 generation
remains readable while a disjoint V2 candidate is built from one authoritative
snapshot plus its retained contiguous tail. Once a V2 candidate is allocated,
the worker prepares that successor before advancing the selected V1 further.
A fully validated candidate may publish its actual frontier H below the current
authoritative head. A replacement must strictly advance the same-specification
published frontier; a prepared candidate that cannot do so waits for newer work
and is replaced when that work arrives. Head movement alone does not discard a
candidate that can advance the published frontier. Cancellation, storage
exhaustion, corruption, or a crash before selection leaves the exact prior
generation selected.

Publication never reports commits above H as applied. Queries requiring a newer
frontier retain their existing bounded wait or typed unavailable result; Latest
and epoch-intersection checks remain authoritative. Retention keeps the tail
above the actual H, and the next bounded rebuild can advance it again. This
removes rebuild invalidation under steady writes without promising that builds
can keep up with an arbitrary write rate.

A V2 generation is an immutable `generation-<u64-hex>` directory containing
only its exact Segment V2 files, partition Manifest V2 files, and `ROOT-V1`.
The root canonically binds the complete ordered partition inventory, generation,
history incarnation, definition, frontier, format tuple, total counts, and a
root-only physical-generation fingerprint. Files are written privately,
synced, checksummed, renamed, and completely reopened before the candidate may
enter durable control. No partition manifest, directory listing, or mixed V1/V2
member can publish a prefix of a generation.

Publication closes new query-view capture, performs the one exact expected-
control and transaction-current-head compare-and-set, then rereads durable
control for every known success, mismatch, storage failure, or uncertain
result. The process installs the exact selected validated immutable view before
capture reopens or readiness is acknowledged. Queries which already captured
the predecessor may finish. A selected V2 root which is missing, corrupt,
partial, mixed, stale-incarnation, or otherwise contradictory keeps the source
closed and degraded; it never falls back to V1.

Cold open, unclean recovery, restore, scrub, and rebuild validate the complete
root, manifests, segment framing, lane encodings, checksums, statistics, and
cross-file identities. A successful open retains immutable process-local
segment and pruning views. Hot queries do not reread, rehash, decode, or
reconstruct proof from durable files. Exact scalar zone maps and complete
canonical dictionaries may reject only a segment proven unable to match;
missing evidence scans. Current row-policy admission is not compiler-proved
segment-aligned, so that policy mode uses the same fixed scan work class and
does not prune from protected statistics. Statistics, encoding choices, and
skip counts are not public diagnostics, logs, or metrics.

V2 compaction emits a new never-reused generation through the same gate and
publishes only after it advances the selected frontier; it never edits or
reuses the selected root. Reclamation is derived
from durable Published/Candidate/Predecessor control pointers and process-local
captured views. An unselected directory is deleted only after neither class
names or holds it, and deletion plus parent-directory sync is crash-idempotent.
The current authoritative mutation model is Create/Replace: deletion and
tombstone row states do not exist yet. V2 therefore preserves the existing V1
supersession, idempotent replay, and holdback semantics without introducing a
different deletion interpretation.

The predecessor vector-control record (durable tag 65) remains readable only
for bounded structural validation through the epoch-1 compatibility window.
It is not writable and contributes no selection, retention, health, recovery,
metrics, or migration input. Its values and old directories are never copied or
translated into common control; WP-757 removes that predecessor family.
