# ADR-0123: Portable Cloud Performance and Compact Command Segments

- **Status:** Accepted
- **Direction approved:** 2026-08-15 (maintainer, optimize before the 72-hour soak)
- **Exact text accepted:** Yes, 2026-08-15 (maintainer)
- **Decision deadline:** Before WP-621 writes writer-frame format V2 or the final
  alpha endurance candidate is built

The maintainer accepted this exact text on 2026-08-15. Any change to codec,
selection, bounds, compatibility, activation thresholds, or authority requires
an explicit accepted amendment.

## Context

The first isolated cloud run changed the performance diagnosis. On the local
Ryzen 7950X and NVMe workstation, RiffDB reached 1.19--1.50 times the
safe-application PostgreSQL comparator through 32 clients. On an otherwise idle
four-core GCP VM with ordinary persistent disk, the same release candidate
reached only 0.55, 0.56, 0.76, and 0.90 times that comparator at 1, 8, 32, and
128 clients. The cloud repetitions were stable and correctness-clean. The
workstation result therefore cannot be the alpha's sole performance proof.

The cloud mechanics probes separate three costs:

1. a pre-zeroed positional `fdatasync` is about 0.48--0.56 ms through 64 KiB,
   so the device's basic synchronous fence is not the 35 ms high-concurrency
   writer duration;
2. the existing canonical writer frame is about 4.7 KiB per representative
   command, and constructing 1,024 commands' frames takes 31--40 ms on the
   cloud CPU while applying the same mutations to the in-memory overlay takes
   about 1.5 ms; and
3. the asynchronous redb checkpoint is correct and already uses one ordered
   transaction at the accepted 4,096-transition/16-MiB trigger, but its
   copy-on-write materialization competes with large foreground journal writes
   on provisioned cloud storage.

ADR-0101, ADR-0103, and ADR-0104 already give RiffDB the correct physical
shape: one checksummed preallocated journal fence acknowledges a group, an
immutable overlay publishes it, and redb checkpoints asynchronously. Replacing
redb or adding another WAL would duplicate the architecture rather than fix the
remaining cost. The next campaign must reduce bytes and serialized construction
work while keeping checkpoint-plus-suffix authority exact.

The per-table census rules out header-only optimization. In the representative
1,024-command segment, fixed mutation headers and keys account for about two
percent of frame bytes. The immutable command-segment value accounts for about
76 percent, with entity and secondary-index post-images accounting for most of
the remainder. Prefix-packing keys could add complexity while leaving the
dominant bytes untouched. The next campaign must compact the command segment
itself while keeping checkpoint-plus-suffix authority exact.

`PERF-017` freezes semantic frame content, frontiers, bounds, and recovery, but
does not require the current verbose mutation encoding forever. ADR-0112 does
require any new durable writer version to have an explicit reader/upgrade
story. The format decision therefore belongs in an accepted ADR rather than an
implementation-only optimization.

## Proposed Decision

### 1. Alpha performance is portable evidence

The final performance gate runs on two recorded hardware classes:

- a high-IPC workstation with local NVMe; and
- a four-to-eight-vCPU general-purpose cloud VM whose database resides on the
  provider's ordinary persistent block storage, not local ephemeral NVMe.

Both profiles run the exact `PERF-018` comparator, durability, client,
correctness, and idle-host rules. The safe-application PostgreSQL comparator is
the release peer; minimal PostgreSQL remains a useful floor and is reported but
is not allowed to erase the application obligations RiffDB absorbs. An alpha
candidate must meet the existing `PERF-008` ratios on both profiles. A faster
workstation result cannot waive a cloud miss, and a cloud pass cannot waive a
workstation regression.

### 2. Measure logical bytes, physical bytes, and checkpoint interference

Fixed-cardinality internal telemetry records, without keys or values:

- logical canonical mutation bytes before writer-frame encoding;
- encoded logical frame bytes and padded physical extent bytes;
- frame construction, positional write, fence, publication, checkpoint apply,
  checkpoint commit, and checkpoint-reclamation durations separately;
- commands and mutations per frame, checkpoint transitions and frames, and
  whether a foreground fence overlapped one in-flight checkpoint; and
- redb checkpoint process-write bytes when the host supplies that counter.

The journal mechanics harness sweeps 4 KiB, 64 KiB, 256 KiB, 1 MiB, 4 MiB, and
the maximum accepted frame, both alone and while a representative checkpoint
write is active. These are diagnostic observations, never application-visible
timing or data-dependent labels.

### 3. A compact command-segment successor carries the same authority

The journal extent header, generation/position binding, zero-filled recyclable
extent, physical frame wrapper, mutation program, footer, tear detection, and
hash-chain rules from ADR-0103 remain byte-identical. The optimization is a
successor of the stored command-segment record selected at merge time from the
next unallocated durable version. WP-621 MUST reconcile that allocation with
the repository-wide durable-version registry before editing Proto or fixtures.

The successor wraps the exact canonical bytes of the current command-segment
body in a closed encoding envelope:

- `Raw` carries the canonical body without transformation;
- `Lz4Block` carries one independent LZ4 block produced by `lz4_flex` exactly
  pinned at `=0.14.0`, with default features disabled and only `std`,
  `safe-encode`, `safe-decode`, and `checked-decode` enabled;
- the envelope carries an independently checked uncompressed byte length and
  SHA-256 digest, the existing semantic segment digest remains over the
  uncompressed canonical body, and the outer stored envelope and journal frame
  continue to checksum/hash the exact stored bytes; and
- the writer chooses `Lz4Block` only when its complete encoded envelope is at
  least 12.5 percent smaller than `Raw`; otherwise `Raw` is canonical for that
  input. Applications and operators cannot select the codec.

Before allocating output, decode rejects an unknown codec, a declared length
above the existing maximum command-segment/frame bound, an impossible encoded
length, or a length that cannot fit the platform's bounded allocation type. It
then requires the decompressor to produce exactly the declared length, verifies
the uncompressed digest, and passes those bytes through the existing complete
command-segment structural, manifest, chain, and semantic-digest validation.
Codec success alone never makes a segment valid.

`lz4_flex` is a new durability-critical dependency and the first accepted
compression codec in an authoritative format. Its exact source/checksum enters
the lockfile and dependency-deny review. It is nameable only from the storage
codec crate, remains under `#![forbid(unsafe_code)]`, uses neither frame
streaming nor dictionaries, and may not be upgraded or have its feature set
changed without durable fixture and recovery review. The LZ4 block format is
the durable contract; decoder compatibility does not depend on reproducing a
historical encoder's byte choices.

No command logic, authorization, policy, clock, ID allocation, index derivation,
or outcome calculation runs during decode or recovery. The live staging path
builds the canonical segment body once, computes its existing digest, and
produces either the raw or compressed stored value once. Reservation charging,
overlay publication, journal writing, and redb checkpoint replay share those
immutable stored bytes; the acknowledgement path never decompresses or
re-decodes the segment it just sealed.

Header/prefix packing may be investigated only after segment compaction. It is
not part of the accepted format change unless separate mechanics demonstrate at
least another ten-percent end-to-end reduction and this ADR is amended.

The compact successor must beat the current writer on the recorded cloud
profile before activation:

- at least 35 percent fewer complete journal-frame bytes and at least 25
  percent fewer checkpointed command-segment bytes for a captured, redacted
  TicketDesk mixed-write corpus (not the repetitive synthetic probe alone);
- at least 20 percent lower combined validation/encoding/staging plus durable
  writer time in the sustained 32-client cloud cell; and
- unary generated-command p50 and smallest-frame CPU no slower than 1.10 times
  the current writer on either hardware profile.

Failure to meet all three leaves the current record authoritative and blocks
WP-621 activation; the implementation may not ship a new durable format for an
immaterial synthetic win.

### 4. Compatibility and activation are explicit

The new binary reads and validates both the existing and compact command-segment
records. A journal suffix and redb checkpoint may contain both record versions;
the journal byte hash chain, semantic segment chain, and exact frontiers remain
continuous across the boundary. New standard-profile writes use the compact
successor only after the release's durable-format manifest selects it. Unknown
record or codec versions fail closed before mutation.

The release manifest records the successor writer and minimum reader. An older
binary encountering that manifest or record refuses with the typed
unsupported-format path; downgrade is not implied. Backup, restore, recovery,
replication-derived reads, and export preserve the same semantic authority.
Export/reimport remains the portable path across a later incompatible alpha
format epoch under ADR-0112.

The change does not alter replication or changelog bytes. ADR-0100 derives
replication frames from published durable frontiers and semantic records, never
from journal extent bytes. That layering is retained and architecture-tested.

### 5. Checkpoint scheduling remains bounded and writer-aware

The accepted 4,096-transition/16-MiB checkpoint trigger, 8,192-transition/
32-MiB suffix ceiling, 128-MiB overlay charge, and 256-transition/16-MiB
unpublished-frame ceilings remain hard safety bounds. No operator or
application knob may raise them.

Within those bounds, the checkpointer may yield between complete table runs
while foreground work is queued, but it may not split a redb transaction,
publish a partial checkpoint, reorder mutations, or delay so long that reserved
headroom is consumed. Foreground admission uses exact encoded and padded bytes,
not command count alone. If the suffix reaches mandatory headroom, the writer
applies typed bounded backpressure and finishes the checkpoint; it never drops
history or acknowledges beyond the proven capacity.

### 6. Warm named-query resolution is one immutable exact lookup

The server may publish one immutable process-local lookup artifact keyed by the
complete contract lineage/version/hash, query-module hash, and operation name.
It contains only already-validated shared contract/module/program ownership.
Warm generated reads may clone that artifact after ordinary request admission
instead of taking multiple catalog/module cache locks.

Cold misses and every deployment/module publication continue through the
existing blocking catalog path. Publication replaces the complete immutable
view atomically; it never mutates an entry in place. Fresh authentication,
begin authorization, row-policy evaluation, pre-release authorization,
post-execution authorization, capability revision checks, freshness fences,
and response redaction remain per request. A cache identity mismatch or
publication race falls back to the authoritative path or fails closed; it never
serves a nearby plan.

## Options Considered

1. **Certify the workstation result:** rejected because it hides the low-IPC,
   persistent-disk deployment class the alpha explicitly supports.
2. **Replace redb:** rejected. The mechanics show the journal/checkpoint design,
   not redb's existence, is the immediate lever, and the current engine remains
   semantically sound.
3. **Prefix-pack frame rows first:** rejected by the census. Headers and keys
   are about two percent of the representative frame and cannot deliver the
   required improvement.
4. **Raise suffix and memory ceilings:** rejected. It improves a short benchmark
   by borrowing unbounded recovery and memory debt from the future.
5. **Compact the dominant exact command segment plus immutable warm resolution
   and portable gates:** selected because it attacks measured bytes in the
   journal and checkpoint with one bounded validation layer, without weakening
   the public safety model.

## Consequences

- The compact command-segment successor adds a permanent decoder, dependency,
  and compatibility fixtures.
- Compression adds bounded encode/decode CPU; the activation gates require its
  net cloud result to be materially positive, including unary latency.
- Checkpoint work remains visible in steady-state evidence rather than being
  postponed beyond a benchmark window.
- The query fast path consumes bounded process memory proportional to deployed
  named operations, capped by the existing catalog/module limits.
- The 72-hour run must restart from the final optimized build. The preserved
  partial pre-optimization soak is diagnostic only.

## Compatibility

No public API, gRPC, MCP, CLI, generated-client, contract source, RiffQL, IR,
outcome, cursor, or application-visible transaction behavior changes.
Existing command-segment records remain readable. The compact successor is a
new internal durable record version declared in the ADR-0112 manifest; old
readers fail closed. Backup/restore retains exact bytes. Replication and export
formats do not change.

## Security

Telemetry contains counts, sizes, fixed stage labels, and durations only; no
key, value, principal, token, contract symbol, or secret-derived bucket is
emitted. Decompression is length-bounded before allocation and followed by
independent length, digest, canonical structure, manifest, and chain checks.
Query caching stores only already-authorized-neutral compiled
artifacts; every principal-specific authorization and redaction safe point is
re-executed. Malformed lengths, codec tags, compressed bodies, overflow, hash
mismatch, noncanonical segments, or trailing bytes fail closed.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** no application-facing option is
  added. Applications cannot select frame versions, checkpoint cadence, cache
  behavior, durability, or bounds. Commands remain typed, idempotent, atomic,
  authorized, and durable before acknowledgement; reads retain all current
  authorization and freshness safe points.
- **Scale:** all frame, suffix, overlay, cache, and checkpoint work retains
  static count and byte ceilings. The decision remains single-node for the POC,
  but replication stays independent because it consumes semantic published
  frontiers rather than physical journal bytes. No full-database rewrite is
  introduced.

## Testing

- Existing segment fixtures remain byte-identical; successor golden fixtures
  cover Raw/Lz4 selection boundaries, incompressible and compressible bodies,
  empty/minimum/maximum bodies, unknown codecs, false lengths, digest mismatch,
  trailing bytes, and decompression bombs.
- Property tests prove both record versions decode to identical command
  segments and ordered mutation programs for generated bounded groups.
- Crash arms cover torn compact records, old-to-new boundaries, mixed suffixes,
  checkpoint during each version, extent recycle, backup/restore, and startup.
- Mechanics evidence records old/successor size and construction on both hardware
  profiles, including checkpoint overlap and maximum-frame cases.
- Concurrent catalog publication tests prove warm lookup exactness, fallback,
  poisoning behavior, and unchanged authorization revocation.
- The final `PERF-018` interactive and write-only matrix runs three 90-second
  repetitions on both profiles before the 72-hour endurance candidate starts.

## Requirements and Work Packages

- **Requirements:** `PERF-007`, `PERF-008`, `PERF-009`, `PERF-015`,
  `PERF-017`, `PERF-018`, `REC-001`, `REC-002`, `STO-001`, `STO-002`
- **Defines or blocks:** WP-620, WP-621, WP-622, WP-623
- **Final evidence:** WP-623, then WP-579

## Decision Deadline

Exact acceptance is required before WP-621 changes durable writer bytes. WP-620
measurement and WP-622's immutable exact query lookup may proceed under the
already accepted performance and authorization requirements. The final alpha
artifact and 72-hour endurance run must wait for WP-623.
