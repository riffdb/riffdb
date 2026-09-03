# ADR-0160: Rebuildable Typed Columnar Segment V2 and Exact Segment Pruning

- **Status:** Accepted
- **Direction approved:** Yes, 2026-09-03 (maintainer, in session)
- **Exact text accepted:** Yes, 2026-09-03 (maintainer, as written)
- **Accepted date:** 2026-09-03
- **Decision deadline:** Before WP-710 adds a columnar layout identity, segment
  statistic, physical encoding, or V2 checkpoint artifact
- **Requires:** ADR-0010, ADR-0017, ADR-0072, ADR-0085, ADR-0086, ADR-0111,
  ADR-0124, ADR-0130, ADR-0152
- **Amends if accepted:** ADR-0086's initial row-framed segment implementation,
  without changing its authority, freshness, policy, or rebuildability semantics
- **Defines or blocks:** WP-710 and WP-711

The maintainer approved the direction and accepted this exact text on
2026-09-03. This record is now authoritative for WP-710 and WP-711.

## Context

The current columnar projection proves the important semantic boundaries: it is
derived and rebuildable, publishes one atomic frontier, survives checkpoint and
replay, enforces organization scope and row policy, and matches an independent
evaluator. Its physical representation is intentionally a feasibility format.
Each immutable segment contains primary-key-sorted row objects; every cell is a
length-framed canonical value, and a query merges the applicable segments and
delta into owned row-shaped values before evaluating predicates or aggregates.

That shape pays row allocation, canonical dispatch, and unrelated-column decode
for every candidate. It also gives the executor no safe way to reject an entire
segment from exact min/max or presence evidence. ADR-0086 deliberately reserved
typed columns, dictionary encoding, zone maps, bitmap structures, and vectorized
execution, but did not freeze their physical or compatibility rules.

The format remains derived, but it is still persistent operational state. A new
binary must not reinterpret an old segment, publish a partially rebuilt layout,
or make a freshness claim from bytes it cannot recover. Statistics may contain
sensitive distribution facts and cannot become a public catalog or an
authorization shortcut. Physical encoding must therefore be versioned,
checksummed, partition-scoped, bounded, and replaceable through the existing
generation lifecycle.

## Proposed Decision

### 1. Introduce an additive V2 derived segment generation

Columnar segment V2 is a new provider-state/layout identity under ADR-0124. V1
bytes and decoders remain exact. One published projection generation uses one
layout version; a manifest may not silently mix V1 and V2 segments in the same
generation.

Upgrade builds a disjoint V2 candidate generation from an authoritative
snapshot plus the retained tail. V1 remains published until the candidate:

- reaches at least the current published frontier;
- matches the independent logical projection at that frontier;
- passes complete header, directory, lane, statistic, and checksum validation;
- records the exact projection definition, history incarnation, organization
  partition, generation, and format identities; and
- is published through the existing atomic generation transition.

A crash before publication leaves V1 authoritative for projected reads. A crash
after publication can reopen only the complete V2 manifest. Unknown, mixed,
partial, stale-incarnation, or contradictory layouts fail closed and trigger the
existing typed rebuild lifecycle. Rebuild never changes authoritative entity,
event, commit, backup, export, or changelog bytes.

### 2. Freeze one bounded typed segment directory

Every immutable V2 segment belongs to exactly one projection definition,
generation, organization partition, and bounded authoritative frontier interval.
Its checked header and directory bind:

- format and encoding-registry versions;
- definition fingerprint, history incarnation, generation, organization key,
  segment identity, row count, column count, and frontier interval;
- a primary-key lane and entity-version lane;
- one directory entry per compiler-declared projected field;
- each lane's logical type, physical encoding, offset, encoded length, value
  count, missing count, null count, and checksum;
- exact typed min/max only for registry types whose total comparison is frozen;
  and
- complete-file length and checksum.

Counts, offsets, lengths, rows, fields, dictionaries, runs, and encoded bytes
all have fixed implementation maxima checked before allocation or indexing.
Directory entries are canonically ordered by stable field ID. Unknown tags,
overlap, gaps forbidden by the framing profile, arithmetic overflow, invalid
counts, noncanonical dictionaries, invalid bit widths, inconsistent statistics,
or checksum disagreement make the segment unusable.

### 3. Use a closed physical encoding registry

The initial V2 registry may contain only independently specified exact
encodings:

- fixed-width big-endian lanes for fixed-width canonical scalar classes;
- offset-plus-bytes lanes for bounded variable-width canonical values;
- sorted canonical dictionaries plus bounded bit-packed ordinals for eligible
  enum and bounded string-like values;
- packed Boolean lanes and separate missing/null validity bitmaps; and
- checked delta encoding for eligible integer and timestamp lanes when every
  reconstructed value remains exact.

Run-length encoding or another encoding may be added only through a successor
registry identity and its own canonical fixtures. General-purpose compression,
native-endian values, floating point, lossy encoding, CPU-feature-dependent
bytes, and application-supplied codecs are excluded.

Encoding selection is deterministic derived-state construction. It uses only
the field type and the bounded candidate segment's measured byte sizes across
the closed registry, selects the smallest exact representation with stable
tag-order tie-breaking, and records the selected tag in the segment. Callers,
contracts, query parameters, tenants, and runtime requests cannot choose an
encoding or require a storage-layout identity.

### 4. Make zone maps exact pruning evidence, never authority

For a type with a frozen total comparison, V2 records exact segment min/max over
present non-null values and exact missing/null/present counts. Predicate
execution may skip a segment only when an independent truth-table proof shows
that no row in the segment can satisfy the compiler-sealed predicate.

Pruning is conservative:

- absent or unsupported statistics mean scan, not guessed rejection;
- missing and explicit null retain their existing distinct semantic states;
- NaN-like or partially ordered values cannot publish min/max evidence;
- dictionaries may prove exact absence only after their complete canonical
  ordering and checksum are validated; and
- multiple predicates may combine only with the immutable query program's
  existing Boolean semantics.

Zone maps never establish authentication, field visibility, row-policy
admission, exact count, or freshness. Current authorization and row policy still
run at their existing safe points. Statistics stay private to the provider and
must not appear in public errors, explain output, MCP resources, logs, or
per-tenant metrics.

For an inference-sensitive row-policy shape, a plan may use segment statistics
only when the provider state is policy-aligned or the compiler proves that the
resulting work class reveals no protected distribution. Otherwise pruning is
disabled for that plan while exact execution remains available within its
declared scan budget.

### 5. Validate durable facts once per process generation

Open/rebuild validates every segment's complete framing, checksums, canonical
encoding invariants, statistics, manifest membership, and definition identity
before publication. The resulting immutable segment view is process-local and
nonserializable. Queries may trust that validated representation and must not
re-read, re-hash, or re-prove durable segment bytes per row or operation.

This is the standing pay-once rule, not weaker integrity. Explicit scrub,
startup after an unclean boundary, checkpoint construction, restore, rebuild,
and corruption tests retain complete validation. A segment file cannot be
mutated in place after validation; replacement always publishes a new immutable
segment and snapshot.

### 6. Preserve bounded compaction and recovery

Compaction reads validated immutable segments and emits a new bounded V2 segment
through a temporary file, complete sync, checksum, and manifest transition. It
never mutates a published segment. Supersession, deletion/retraction, holdback,
idempotent replay, visible/durable frontier separation, retention fencing, and
generation retirement retain ADR-0086 semantics.

Derived V2 files are excluded from backup identity only when ADR-0086's complete
source-closure rule proves them rebuildable. Otherwise the format manifest and
backup unit must carry them explicitly. ENOSPC, cancellation, corrupt input, or
an exceeded format bound leaves the prior generation readable and returns a
typed degraded/rebuild outcome; it never advances the durable frontier.

### 7. Require mechanics evidence before production activation

WP-710 first records the current V1 byte, allocation, CPU, and examined-row
ledger over fixed projection workloads and builds an independent V2 codec. V2
production activation proceeds in WP-711 only when the codec demonstrates:

- byte-exact logical equivalence and crash-safe recovery;
- no more than 1.10 times V1 bytes for high-cardinality incompressible data;
- at least 25 percent fewer segment bytes on the registered low-cardinality
  corpus;
- at least 90 percent correct segment rejection on the registered 1-percent
  selective clustered corpus, with zero false negatives; and
- no greater than five percent regression for a full-scan decode before the
  vectorized executor exists.

Failed mechanics remain as value-free evidence and do not rotate the production
layout. Performance thresholds are implementation gates, not public promises.

## Options Considered

1. **Keep row-framed segments and optimize allocations:** rejected as the main
   direction because it cannot provide column pruning or contiguous batch input.
2. **Use an external columnar format directly:** rejected for the first V2
   because its semantics, dependency graph, feature surface, and compatibility
   would become part of RiffDB's trusted boundary without a demonstrated need.
3. **Rewrite V1 files in place:** rejected because crashes and mixed readers
   could observe an unprovable generation.
4. **Add a versioned typed derived format with disjoint rebuild:** proposed
   because authoritative bytes remain untouched and rollback is the prior
   published generation.

## Consequences

- Selective and aggregate workloads gain exact segment pruning and contiguous
  typed inputs for later batch execution.
- Low-cardinality projections use materially less derived disk and memory.
- The columnar engine acquires a real format registry, compatibility fixtures,
  rebuild ceremony, and more corruption cases.
- V1 remains readable while V2 adoption occurs through generation rebuild, not
  silent reinterpretation.
- Vectorized execution, parallel segment scheduling, bitmap indexes, rollups,
  top-K state, approximate sketches, and public query changes are not implemented
  by this ADR.

## Compatibility

Authoritative storage, events, changelog, backup identity, RiffQL source, query
IR, query modules, plans, cursors, generated clients, gRPC, MCP, and application
locks remain byte-exact. V2 adds only rebuildable columnar provider-state and
manifest identities registered under ADR-0124. V1 remains readable; new V2
writes are least-sufficient and occur only in a V2 candidate generation.

## Security

Segment bytes and statistics are internal derived state under the same file and
directory protections as current projections. They confer no capability and
cannot widen field or row visibility. Statistics and encoding choices are
redacted from public failures and tenant-visible timing classes. Policy-aligned
state or compiler proof is mandatory before data-dependent pruning under an
inference-sensitive policy.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** applications still invoke only
  compiled named or bounded projected operations. They cannot select layout,
  encoding, zone-map use, rebuild, fallback, validation mode, policy, freshness,
  or resource ceiling. Missing proof scans within the existing bound or fails
  typed; it never guesses or returns partial data.
- **Scale:** segments, directories, lanes, dictionaries, bitmaps, compaction,
  rebuild turns, validation, and diagnostics have fixed byte/cardinality bounds.
  Work is partition- and segment-scoped; upgrade streams into a disjoint
  generation and requires no database-wide in-memory rewrite or co-located
  authoritative storage.

## Testing

- Canonical V1 and V2 format fixtures plus version-topology checks.
- Independent per-encoding round-trip/property tests at every size, offset,
  value, missing/null, dictionary, delta, and checksum boundary.
- Zone-map truth-table and randomized differential tests proving zero false
  negative pruning for every admitted predicate and optional-value state.
- Torn header/directory/lane/checksum/manifest, ENOSPC, cancellation, compaction,
  mixed-version, stale-incarnation, and publish-boundary crash matrices.
- V1-published/V2-building restart tests and byte-exact logical equivalence at
  matched frontiers.
- Architecture checks proving validation is open/publication scoped, segment
  files are immutable, statistics are nonpublic, and no request selects encoding.
- Fixed workload byte/CPU/allocation/pruning receipts before activation.

## Requirements and Work Packages

- **Requirements:** `PRJ-001` through `PRJ-004`, `OQ-017` through `OQ-024`,
  `OQ-053`, `PERF-001`, `PERF-007`, `PERF-008`, `PERF-018`
- **Format and mechanics:** `WP-710`
- **Lifecycle and pruning activation:** `WP-711`
- **Final evidence:** `WP-715`

## Decision Deadline

Exact human acceptance is required before WP-710 adds a layout tag, manifest
successor, segment encoding, canonical statistic, topology entry, or checked
fixture. Any need to change authoritative storage or public query semantics
returns for separate review.
