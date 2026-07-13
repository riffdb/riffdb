# ADR-0017: Projection Group Keys, Generations, and Durable Frontiers

- **Status:** Accepted
- **Direction approved:** 2026-07-13
- **Exact text accepted:** Yes
- **Accepted:** 2026-07-13
- **Requires:** ADR-0004 accepted before or in the same governance change
- **Amends:** ADR-0010 projection identity, generation, idempotency, lifecycle,
  and rebuild scope; ADR-0011 typed-key and unkeyed hash-domain registries
- **Clarifies:** ADR-0013, ADR-0016, and SPEC Sections 10.2 and 15
- **Decision deadline:** Before WP-040 freezes projection group schemas, WP-060
  freezes projection ports, or WP-070 freezes redb tables

This record closes a sequencing gap discovered before storage implementation.
WP-070 owns the production redb adapter, while WP-170 is not allowed to change
that adapter. Projection key and generation semantics therefore cannot first be
chosen in WP-170. The human maintainer accepted this exact record on 2026-07-13
as part of the atomic semantic-interface governance batch.

## Context

Accepted ADR-0010 requires projection state and its frontier to update atomically,
requires every commit sequence to be accounted for, and requires state to be
rebuildable without a frontier decrease. Accepted ADR-0013 permits decimal and
money projection group values. Accepted ADR-0016 deliberately does not assign a
projection key envelope and forbids reusing entity, conflict, partition, or index
keys.

The durable identity also needs more than a bare `ProjectionId`. Stable numeric
IDs are scoped to one contract lineage, and an unchanged projection should retain
state across an unrelated compatible contract deployment while a changed plan
must rebuild. A same-plan rebuild needs a new physical generation so it can start
from the beginning without lowering the published frontier or exposing partially
rebuilt rows.

## Decision

### Ownership and projection identity

`riffdb-types` owns these bounded, opaque foundational values:

```text
ProjectionIdentity
  contract_lineage: ContractLineage
  projection_id: ProjectionId
  plan_hash: ProjectionPlanHash

ProjectionGeneration(u64)
ProjectionGroupKey
ProjectionGroupPrefix
ProjectionFrontierKey
ProjectionApplyKey
ProjectionApplyHash([u8; 32])
```

`ProjectionGeneration` is nonzero. Generation 1 is the first build for one exact
`ProjectionIdentity`; later rebuilds allocate the next value with checked
addition. Generation values are scoped to an identity, are never reused, and do
not cross a plan-hash change.

The identity deliberately excludes application contract version and complete
bundle hash. An unrelated compatible deployment may reuse an unchanged
projection because its `ProjectionPlanHash` is unchanged. A semantic projection
change produces a different plan hash and therefore a disjoint state namespace.
Contract lineage prevents equal numeric IDs and equal plan bytes in independent
lineages from aliasing.

`riffdb-contract-ir` owns `ProjectionGroupSchema`, schema-directed group-key
validation, expression-to-component derivation, maximum-size analysis, and
checked decoding. The schema contains its nonzero `ProjectionId`, group and
measure types, codecs, and bounds, but it does not contain
`ProjectionPlanHash` or `ProjectionIdentity`. Its canonical bytes are part of
the projection-plan hash preimage, so embedding that resulting hash would be
self-referential. After the plan hash is computed or verified, the checked bundle
exposes a `BoundProjectionGroupSchema` that pairs the exact schema with its
`ProjectionIdentity`; storage accepts only that bound checked view.

`riffdb-storage-api` owns projection control/state records, apply-marker records,
the complete checked `ProjectionApplyRequestV1`, its canonical encoder/hash
operation, and typed atomic operations. `riffdb-types` owns only the opaque
`ProjectionApplyHash` digest type and the registered domain-separated hash
primitive; it cannot validate or construct a semantic apply request without the
IR schema and storage records. Storage engines persist those types but do not
evaluate projection plans, invent generations, select active plans, or expose
raw writes.
`riffdb-projection` owns event evaluation, lifecycle orchestration, rebuilds,
queries, and waits. `riffdb-proto` remains the only owner of durable and public
Protobuf messages.

`WP-060` therefore has a hard dependency on `WP-040`. The storage API consumes
the checked projection schema and validation evidence owned by
`riffdb-contract-ir`; it does not duplicate them. `riffdb-contract-ir` remains
independent of storage, so this dependency creates no crate cycle.

### Exact identity bytes

The shared v1 projection-identity payload is:

```text
u32_be contract_lineage_utf8_length
+ exact ContractLineage UTF-8 bytes
+ ProjectionId as u32 big endian
+ ProjectionPlanHash as exactly 32 bytes
```

The lineage is nonempty and at most 256 bytes under ADR-0011. `ProjectionId` is
nonzero under ADR-0013. The hash is the typed ADR-0014 projection-plan digest.
There is no display name, application version, bundle hash, compiler version,
generation, tenant, partition, or environment in this payload.

### Durable key envelopes

ADR-0011's typed-key envelope registry gains three accepted entries:

| Key | Prefix | Remaining bytes |
|---|---|---|
| Projection apply marker | `0x41 0x01` | projection identity payload, generation, commit sequence |
| Projection frontier/control | `0x46 0x01` | projection identity payload |
| Projection group state | `0x47 0x01` | projection identity payload, generation, group components |

The purpose bytes are ASCII `A` for projection application, ASCII `F` for
frontier/control, and ASCII `G` for group state. The second byte is key format
version 1.

A complete projection state key is:

```text
0x47 0x01
+ projection identity payload
+ ProjectionGeneration as u64 big endian
+ for each group component in declared source order:
     u32_be canonical_component_length
     + exact ADR-0011 CanonicalValue encoding v1 bytes
```

Each length is nonzero and covers exactly one complete canonical value document,
including its `0x01` value-format byte and scalar tag. Length framing is retained
even for fixed-width values so component boundaries and exact-prefix range ends
remain independent of decoder implementation details.

The frontier/control key is exactly `0x46 0x01` followed by the identity payload.
It has no generation because its value owns the published and optional candidate
generation pointers for that identity.

The apply-marker key is exactly:

```text
0x41 0x01
+ projection identity payload
+ ProjectionGeneration as u64 big endian
+ nonzero CommitSequence as u64 big endian
```

There is exactly one marker for every applied sequence of every generation,
including a sequence with no relevant event and therefore no row update. Apply
markers are retained for the POC lifetime of their generation. Compaction or
marker garbage collection for a retained generation requires a later decision
that preserves duplicate equality and rebuild evidence. A future collector may
delete one already-retired generation only as a whole under a separately
accepted crash-safe policy; partial retired cleanup is not defined here.

All three complete key forms are at most 4,096 bytes. Reconstruction checks the
purpose, version, lineage bound and UTF-8, nonzero projection ID, exact hash
length, nonzero generation where present, nonzero commit sequence where present,
checked component lengths, full input consumption, and total bound before
returning a typed value. Semantic component count and type validation additionally
requires the exact checked `BoundProjectionGroupSchema` for the same identity.

### Group component codec

Projection grouping deliberately uses framed ADR-0011 canonical scalar values,
not the ADR-0016 ordered entity-key component payloads. This is required because
grammar version 1 permits decimal and money group values, which ADR-0016 excludes
from authoritative entity, partition, conflict, and index keys.

The immutable v1 projection group registry is:

| IR scalar | Required canonical value |
|---|---|
| Boolean | Boolean |
| `i64` | signed 64-bit integer |
| `u64` | unsigned 64-bit integer |
| `decimal<P,S>` | decimal with exactly `P` and `S` |
| `money<CURRENCY>` | money with the exact currency, precision, and scale |
| `string<N>` | UTF-8 string whose byte length is at most `N` |
| `bytes<N>` | bytes whose length is at most `N` |
| timestamp | canonical timestamp |
| date | canonical date |
| UUID | canonical UUID |
| declared enum | matching `EnumTypeId` and a declared `EnumVariantId` |

Optional, null, list, record, and every other value are invalid group
components. There is no coercion, normalization, case folding, stringification,
hash substitution, scale conversion, or currency conversion.

Canonical group bytes provide deterministic equality and a stable byte order for
pagination. This ADR does not claim that their lexicographic order is numeric,
calendar, locale, or application sort order. POC projection queries support only
complete-key equality and exact leading-component prefix scans, not inequality,
between, suffix, or application-order scans.

### Canonical projection-apply hash

ADR-0011's unkeyed hash-domain registry is extended with the typed
`ProjectionApplyHash` domain `riffdb.projection-apply/v1`. It uses the unchanged
ADR-0011 SHA-256 frame. The canonical domain payload is:

```text
ASCII "RIFFDB-PROJECTION-APPLY" + 0x00
+ u32_be apply codec version, exactly 1
+ u32_be projection identity payload length
+ exact projection identity payload
+ ProjectionGeneration as u64 big endian
+ nonzero CommitSequence as u64 big endian
+ expected frontier tag: BeforeFirst 0x00 or AppliedThrough 0x01
+ when AppliedThrough, expected nonzero CommitSequence as u64 big endian
+ u32_be row update count
+ for each row update in strictly increasing complete ProjectionGroupKey order:
     u32_be complete group-key length
     + exact complete ProjectionGroupKey bytes
     + prior evidence tag: Absent 0x00 or Present 0x01
     + when Present, prior nonzero last_changed_sequence as u64 big endian
     + u32_be canonical measure-record length
     + exact ADR-0011 CanonicalValue record bytes for the post-image measures
```

The identity, generation, and sequence repeat the apply-marker key and must match
it. `BeforeFirst` is valid only for sequence 1; otherwise `AppliedThrough` must be
the exact predecessor of the applied sequence. Every group key must repeat that
same identity and generation and validate under the exact
`BoundProjectionGroupSchema`. The row count is at most 4,096. Duplicate or decreasing
group keys, unknown frontier or prior tags, zero assigned sequences, a non-record
or schema-invalid measure post-image, trailing bytes, or a count/length/bound
mismatch reject before hashing. The post-image's `last_changed_sequence` is the
top-level sequence and is therefore not redundantly encoded inside the measure
record. An empty update batch has row count zero and still produces a distinct
hash bound to its identity, generation, sequence, and expected frontier.

Encoding and hashing are streaming and checked; an implementation does not need
to allocate the complete preimage. Only
`ProjectionApplyRequestV1::canonical_hash` in `riffdb-storage-api`, operating
with the exact `BoundProjectionGroupSchema`, constructs the semantic digest.
`riffdb-types` supplies the domain-separated byte-hash primitive and opaque
result type but no API that can bless unchecked row updates as a valid request.
The apply request and marker store only the typed 32-byte hash, never a
caller-supplied untyped digest. This is idempotency evidence for deterministic
derived-state application, not a signature or an authorization proof.

### Schema and bounds

One projection has one nonempty ordered group schema with at most 1,024
components, matching the existing grammar/IR tuple bound. A schema records:

- its nonzero `ProjectionId`; the checked bound view adds the exact
  `ProjectionIdentity` only after plan hashing;
- group codec version 1;
- the ordered exact scalar types and enum identities;
- each string/byte declared maximum;
- the checked maximum encoded size of every framed component; and
- the checked maximum complete key size including identity and generation.

Compilation fails if the maximum complete key can exceed 4,096 bytes, even if a
particular observed event would fit. Runtime construction checks the same limit
and exact schema before allocation. The calculation includes every four-byte
component length, canonical value version/tag bytes, decimal precision/scale,
money currency bytes, and the complete identity/generation envelope.

Compilation checks the maximum complete stored state record as well as the key.
The identity, generation, repeated group components, measure record, sequence,
record framing, and durable-envelope headroom must fit the limits below; a
contract is rejected rather than relying on observed small values.

One stored projection state semantic payload is at most 1 MiB. One
`ProjectionApplyRequestV1` contains at most 4,096 distinct group-row updates and
at most 15 MiB of canonical semantic content. Its complete staged state rows,
marker, and control write set is at most 16 MiB. One apply snapshot is at most 16
MiB. One query returns at most 500 rows and at most 4 MiB of encoded row content.
Every constituent durable envelope remains under ADR-0006's absolute 16 MiB
ceiling. Counts and byte totals are checked before allocation or opening a write
transaction. Lower limits from policy or configuration may reduce a caller's
result but cannot enlarge these hard limits.

### Complete and prefix query keys

`ProjectionGroupKey` is durable and contains every declared component.
`ProjectionGroupPrefix` is transient and contains the same `0x47 0x01` identity
and selected published generation followed by zero through all complete leading
components. It is never persisted as a state-row key and cannot end inside a
component length or value.

The service derives the identity from the active checked bundle; callers do not
supply an identity or generation as a raw value. Caller group values are
schema-validated and policy-checked into a generation-neutral selector. The
storage query binds that selector to the published generation and constructs the
durable prefix inside its one read transaction. A zero-component selector means
a bounded scan of the projection's published generation. Results are ordered by
complete canonical group-key bytes and paginated with ADR-0007's opaque
policy-bound cursor over the lower continuation defined below. This ADR does not
define the public token encoding.

The storage range is the byte prefix and its lexicographic exclusive successor.
Construction uses checked bytewise successor logic and never appends a guessed
sentinel. Because the first byte is `0x47`, a valid projection prefix always has
an exclusive successor. A complete-key lookup uses exact equality rather than a
range.

### Durable state and control records

The semantic `StoredProjectionStateV1` contains exactly:

1. `ProjectionIdentity`.
2. Nonzero `ProjectionGeneration`.
3. Group component values in declared order.
4. A canonical projection-result measure record ordered by stable `FieldId`.
5. The commit sequence that most recently changed this row.

The repeated identity, generation, and group values must reconstruct the table
key exactly. A mismatch is corruption. A row is created on the first relevant
event for its group. Grammar v1 has no retractions, so rows are not removed by
normal event application. Count and sum updates use the exact checked arithmetic
and measure types in the validated projection plan.

The semantic `StoredProjectionApplyV1`, keyed by `ProjectionApplyKey`, contains
exactly:

1. `ProjectionIdentity`.
2. Nonzero `ProjectionGeneration`.
3. Nonzero applied `CommitSequence`.
4. The exact `ProjectionApplyHash` for the canonical apply request.

The repeated identity, generation, and sequence must reconstruct the marker key
exactly. Marker creation, row post-images, and frontier advancement are one atomic
transaction. For each currently retained published or candidate generation, the
control frontier and marker prefix are reciprocal: `BeforeFirst` has no marker,
and `AppliedThrough(N)` has exactly one marker for every sequence `1..=N` and no
marker above `N`. A violation is corruption.

Publication or failed-candidate replacement can make an older allocated
generation unreferenced by control. Such a generation is retired: it is never
queried, resumed, reused, or accepted as an apply target. Its rows and markers
remain inert derived data until a later whole-generation garbage-collection
decision. Because control deliberately no longer stores its frontier, retained
retired markers do not require a matching control frontier and a missing retired
tail is not an authoritative-state corruption. Their keys and values must still
decode canonically; a generation above `highest_allocated_generation`, or an
apply to any retired generation, fails closed. This exception does not weaken
the reciprocal marker/frontier invariant for a currently retained generation.

The semantic `StoredProjectionControlV1`, keyed by `ProjectionFrontierKey`,
contains exactly:

1. `ProjectionIdentity`.
2. Highest allocated generation, initially 1.
3. Optional published generation and its `FrontierPosition` from ADR-0004.
4. Optional candidate generation and its `FrontierPosition`.
5. Optional `PublishedApplyMode`, present exactly when a published generation is
   present.
6. One closed lifecycle value.
7. Optional `ProjectionFailureV1`.

`FrontierPosition::BeforeFirst` means that generation has applied no application
commit. `AppliedThrough` always contains a nonzero assigned `CommitSequence`.
`CommitSequence(0)` is not used as a semantic sentinel. The repeated identity
must reconstruct the table key exactly.

Absence of a control record is a normal transient state for an exact projection
identity newly exposed by the active checked bundle. It means uninitialized, not
corrupt and not empty-ready. The projection worker creates generation 1 through
the insert-if-absent transition below. A public query or status read for that
known identity maps absence to `Building` at `BeforeFirst` with no generation and
no rows. An identity not present in the checked active or explicitly selected
historical bundle remains a not-found/validation result and is not converted to
`Building`.

The service and public semantic results also use `FrontierPosition` for
`Ready.frontier`, `WaitTimedOut.current`, and `Degraded.current`. `BeforeFirst`
orders below every real commit sequence. An `after_sequence` requirement remains
a real nonzero `CommitSequence`; it never accepts or emits a fabricated zero.
Acceptance requires SPEC Section 15.3 to use this representation explicitly.

Lifecycle tags are building `0x01`, catching up `0x02`, ready `0x03`, degraded
`0x04`, rebuilding `0x05`, and invalid `0x06`. Zero and unknown tags reject.
`PublishedApplyMode` is enabled `0x01` or suspended `0x02`; zero and unknown tags
reject. Enabled permits the retained published generation to consume the next
sequence only in a lifecycle that otherwise permits published application.
Suspended records that the published generation itself failed and cannot resume;
only successful atomic publication of a replacement candidate returns the new
published generation to enabled mode.

`ProjectionFailureV1` contains the nonzero affected `ProjectionGeneration`, one
closed `ProjectionFailureCode`, and an optional nonzero `at_sequence`. The
affected generation must equal the retained published or candidate generation
pointer whose processing failed. The initial code registry is arithmetic overflow
`0x01`, malformed durable event `0x02`, missing commit `0x03`, plan or schema
unavailable/mismatch `0x04`, projection-state integrity failure `0x05`, and hard
limit exceeded `0x06`. Zero and unknown failure codes reject. A failure tied
to evaluation of an existing commit includes that sequence; a plan-lookup or
pre-application limit failure omits it. Generation allocation exhaustion is a
non-writing control-operation result below, not a failure falsely attributed to
a healthy generation. The failure contains no arbitrary string, source
value, operand, group key, event payload, or engine error. Temporary storage
unavailability does not durably rewrite lifecycle state by itself.

Every decoded control record satisfies these structural invariants before its
lifecycle-specific shape is checked:

- `highest_allocated_generation` is nonzero;
- every retained pointer is nonzero and at most the highest allocated generation;
- published and candidate generations, when both present, are distinct;
- a candidate is the highest allocated generation;
- every allocated generation in `1..=highest_allocated_generation` that is not
  the published or candidate pointer is retired and can never become retained
  again;
- published apply mode is present exactly when a published pointer is present;
- an active lifecycle has no failure, while `Degraded` and `Invalid` require one.

The closed durable shapes are:

| Lifecycle | Published | Candidate | Published apply mode | Failure |
|---|---|---|---|---|
| `Building` | Absent | Present and equal to highest allocated | Absent | Absent |
| `CatchingUp` | Absent | Present and equal to highest allocated | Absent | Absent |
| `Ready` | Present and equal to highest allocated | Absent | Enabled | Absent |
| `Rebuilding` | Present below highest allocated | Present and equal to highest allocated | Enabled or suspended | Absent |
| `Degraded` | The exact pointer shape retained from the failed active state | The exact pointer shape retained from the failed active state | Absent with no published pointer; otherwise retained, except a failure affecting published forces suspended | Present and naming one retained pointer |
| `Invalid` | The exact pointer shape retained from `Degraded` | The exact pointer shape retained from `Degraded` | Retained exactly from `Degraded` | Present and naming one retained pointer |

`Degraded` and `Invalid` therefore retain at least one generation pointer. They
never discard or silently replace the rows, frontier, marker prefix, or failure
evidence that caused the state. An old failed candidate may cease to be the
control record's candidate only when an explicit recovery transition atomically
allocates and installs a newer candidate; its historical rows and markers remain
durable.

The closed application permissions by lifecycle are:

| Lifecycle | Generation that may apply | Other application |
|---|---|---|
| `Building` | None; transition to `CatchingUp` before scanning | Reject |
| `CatchingUp` | Candidate | Reject |
| `Ready` | Published | Reject |
| `Rebuilding` | Candidate, plus published only when published apply mode is enabled | Reject |
| `Degraded` | None until an explicit recovery transition | Reject |
| `Invalid` | None | Reject |

Failure persistence names the affected generation and validates that generation's
expected frontier in the same control transaction. A failure cannot degrade or
advance a different generation because control changed concurrently. Failure of
the published generation atomically changes published apply mode to suspended;
failure of a candidate retains the existing published mode.

The v1 lifecycle transition registry is:

| From | Operation and precondition | To |
|---|---|---|
| No control record | Create generation 1 as `BeforeFirst`; identity must be absent | `Building` |
| `Building` | Start the initial contiguous scan; pointers unchanged | `CatchingUp` |
| `Building` | Transaction-current authoritative head is `BeforeFirst`; publish the untouched candidate atomically and set published apply mode to enabled | `Ready` |
| `CatchingUp` | Candidate frontier exactly equals the transaction-current authoritative head; publish candidate atomically and set published apply mode to enabled | `Ready` |
| `Ready` | Allocate checked `highest + 1` as a `BeforeFirst` candidate and retain enabled published application | `Rebuilding` |
| `Rebuilding` | Candidate frontier is not behind the published frontier and exactly equals the transaction-current authoritative head; publish candidate, clear candidate, retire the replaced generation, and set the replacement published mode to enabled | `Ready` |
| Any active lifecycle | Persist a closed failure against one currently retained generation and its expected frontier | `Degraded` |
| `Degraded`, no published pointer | Allocate checked `highest + 1` as a `BeforeFirst` candidate, retire the failed candidate, and clear failure | `Building` |
| `Degraded`, failure affects candidate and published is retained | Allocate checked `highest + 1` as a `BeforeFirst` candidate, retire the failed candidate, clear failure, and retain the current published apply mode | `Rebuilding` |
| `Degraded`, failure affects published and a candidate is retained | Keep that candidate, clear failure, and keep published application suspended | `Rebuilding` |
| `Degraded`, failure affects published and no candidate is retained | Allocate checked `highest + 1` as a `BeforeFirst` candidate, clear failure, and keep published application suspended | `Rebuilding` |
| `Degraded` | Prove the exact plan or authoritative contiguous log required for rebuild is unavailable under POC recovery policy | `Invalid` |

Every transition is one compare-and-set control transaction over the exact prior
identity, lifecycle, pointers, frontiers, published apply mode, highest
generation, and failure. Publication additionally reads the transaction-current
last assigned application sequence and requires the candidate frontier to equal
that `FrontierPosition`; it does not trust a caller-claimed catch-up target. An
apply transaction changes only its target frontier and state/marker rows; it does
not change lifecycle. `Invalid` has no v1 outgoing transition. Recovery from
`Invalid` requires activating a different projection identity or restoring the
whole database from a verified offline backup; fabricating authoritative commits,
plans, or derived rows is forbidden. Unknown transitions and structurally invalid
control combinations fail closed without a partial write.

### Public lifecycle and query mapping

The API-neutral projection query result retains ADR-0010's four result classes:
`Ready`, `WaitTimedOut`, `Degraded`, and `Invalid`. It replaces the illustrative
free-form `reason: String` in SPEC Section 15.3 with one closed safe reason:

```text
ProjectionUnavailableReason
  Building                         tag 0x01
  Rebuilding                       tag 0x02
  Failure(ProjectionFailureCode)   tag 0x03
```

Zero and unknown reason or nested failure tags reject. Protocol adapters may add
only static text selected from this enum; they never relay a stored or engine
error string.

The exact lifecycle mapping is:

| Durable lifecycle | Query result and visible position |
|---|---|
| No control record for a known checked identity | `Degraded { current: BeforeFirst, reason: Building }`; no generation and no rows |
| `Building` or `CatchingUp` | `Degraded { current: candidate frontier, reason: Building }`; no rows |
| `Ready` with frontier behind a requested sequence when its bounded wait expires | `WaitTimedOut { required, current: published frontier }`; no rows |
| `Ready` otherwise | `Ready { data from published generation, frontier: published frontier }` |
| `Rebuilding` | `Degraded { current: published frontier, reason: Rebuilding }`; no rows |
| `Degraded` | `Degraded { current: affected generation frontier, reason: Failure(code) }`; no rows |
| `Invalid` | `Invalid { reason: Failure(code) }`; no rows |

The POC deliberately does not serve the retained published rows during
`Rebuilding`; returning a typed unavailable result avoids implying that a rebuild
is healthy. A future decision may permit serving the published generation but may
not silently change this v1 behavior. `after_sequence` never turns an unavailable
lifecycle into `Ready`. A waiter rereads one complete current query snapshot after
every wake and returns the row set and frontier from that same storage read
transaction.

The API-neutral projection-status result is also closed and safe to expose. It
contains the exact `ProjectionIdentity`, lifecycle, optional published
`(ProjectionGeneration, FrontierPosition)`, optional candidate
`(ProjectionGeneration, FrontierPosition)`, optional `PublishedApplyMode`,
optional closed safe failure status copied from its affected generation, code,
and optional sequence, and the transaction-current authoritative head as
`FrontierPosition`. It does not expose a durable record/envelope or any raw row,
key, plan, event, or engine error. No control record maps to lifecycle `Building`,
no generation pointers or failure, and authoritative head as observed in the same
read transaction. The status shape obeys the same lifecycle invariants as durable
control; adapters do not invent another lifecycle or free-form error model. Lag
is a checked display derivation from the visible frontier and authoritative head,
not separately persisted state.

The exact durable Protobuf messages, field numbers, descriptor hashes, and
golden bytes are added by formal WP-065 after WP-060 accepts the semantic records
and before WP-070 persists them. The exact public query/status/frontier,
lifecycle, failure, pagination, and unavailable-result messages are added by
formal WP-127 after WP-120 accepts the API-neutral service types and before
WP-130 implements gRPC. Both packages are proto-owner interface work under
ADR-0006; neither reopens WP-020's completed phase-zero baseline. No crate may
use an ad hoc serializer, persist an unversioned value, or guess a public field
number in the interim.

### Contiguous application and atomicity

For one identity and generation, `BeforeFirst` accepts only application sequence
1. Thereafter an application batch is accepted only for the exact successor of
that generation's `AppliedThrough` value. A log whose first record is not 1 or
whose prefix has a gap is an integrity failure, not a new projection starting
point.
The worker reads commit records in ascending order and submits one semantic batch
per sequence, including an empty row-update batch when no event is relevant.
Within one commit it evaluates durable events in increasing zero-based
`EventId` ordinal and projection measures in stable `FieldId` order. Multiple
effects for one group are folded in that order into one post-image before row
updates are key-sorted for hashing and storage. Checked arithmetic therefore has
one reproducible overflow point independent of map iteration or worker schedule.

The exact `PRJ-003` idempotency identity is
`(ProjectionIdentity, ProjectionGeneration, CommitSequence)`, with
`ProjectionApplyHash` equality. A same-plan rebuild intentionally applies the
same commit sequence to a disjoint generation, and a changed plan applies it to a
disjoint identity; neither is a duplicate of the old generation. This explicitly
amends SPEC Section 15's illustrative `(projection_id, commit_sequence)` wording
without weakening duplicate equality within one executable derived-state
instance.

Before evaluating a sequence, the worker obtains one bounded
`ProjectionApplySnapshot` for the identity, generation, and exact canonical set of
group keys it will update. One storage read transaction returns the expected
frontier and, for every requested key, either absence or the complete current row
and its `last_changed_sequence`. Candidate generations are readable through this
internal operation so a build or rebuild can resume after process restart.

Each computed post-image carries its expected prior evidence: row absence or the
observed `last_changed_sequence`. The worker computes the canonical
`ProjectionApplyHash` from that complete request before submission. In one
storage write transaction, a next-sequence projection application:

1. Rechecks the exact control identity, lifecycle, target generation, and
   expected prior frontier.
2. Rechecks that every supplied row has the same identity/generation, a valid
   complete key under the checked schema, group values exactly decoded from that
   key, schema-valid measures, and `last_changed_sequence` equal to the applied
   sequence.
3. Rechecks every row's expected absence or `last_changed_sequence`.
4. Recomputes and checks the typed apply hash from the complete bounded request.
5. Requires the exact apply-marker key to be absent.
6. Applies all canonically key-sorted row post-images for that sequence.
7. Creates the exact apply marker.
8. Advances only the matching published or candidate frontier to that sequence.
9. Commits state, marker, and control together or changes none.

Expected prior evidence does not repeat the prior measure bytes in the hash. The
exact frontier compare serializes every legal row change for that identity and
generation, and no other storage port can mutate those rows; therefore an
unchanged expected frontier plus matching absence/`last_changed_sequence` is the
complete optimistic evidence. Integrity tooling may additionally checksum
durable bytes but cannot substitute for this semantic check.

Storage receives validated row post-images, prior-row evidence, and
expected-frontier evidence. It does not execute aggregation expressions. For any
new row write it constructs or validates the repeated stored identity,
generation, group values, and sequence from the hash-covered key, measures, and
top-level sequence; there is no unhashed caller-controlled field in the stored
post-image. For any
submitted sequence at or below the stored frontier, storage recomputes the request
hash and loads that sequence's exact marker. It returns typed `AlreadyApplied`
without writing only when marker identity, generation, sequence, and hash all
equal the request and the authoritative commit remains available. A missing marker
inside the frontier, hash mismatch, conflicting generation, mismatched row,
missing authoritative commit, gap, or frontier inversion is an integrity failure
and writes nothing. This equality rule applies even after later row post-images
have replaced the state produced at the duplicate sequence.

Application to a ready published generation keeps it queryable only after the
state rows and frontier are durable together. A public query obtains lifecycle,
published generation, published frontier, bounded rows, and pagination cursor in
one storage read transaction; it cannot join a control read to a later row scan.
Wait notifications happen after the storage commit and are hints; a waiter rereads
that complete published snapshot before returning. Cancellation or process death
before commit leaves the prior state; after commit, retry observes the advanced
frontier.

### Initial build, plan change, and same-plan rebuild

An initial build creates generation 1 as the candidate with no published
generation and lifecycle `Building`, then `CatchingUp`. It scans the authoritative
log from the beginning. When the candidate equals the transaction-current
authoritative head, publishing that generation, its frontier, and lifecycle
`Ready` is one atomic control-record transition. Queries do not expose candidate
rows.

A projection plan-hash change creates a different `ProjectionIdentity` and starts
its generation 1. Old identity state remains historical and is not returned by
active-contract queries. Automatic old-plan garbage collection is deferred.

A same-plan rebuild allocates the next generation as a candidate, retains the
published generation and frontier, and enters `Rebuilding`. Candidate rows are
written in their disjoint generation namespace. Public queries return a typed
degraded/rebuilding result rather than candidate data. The candidate may replace
the published generation only in one control transaction and only when its
frontier is not behind the previously published frontier. The published frontier
therefore never decreases. Old generation rows remain until after publication;
POC garbage collection may retain them indefinitely.

`PRJ-001` monotonicity is scoped to the published frontier of one exact
`ProjectionIdentity`. A same-plan generation replacement cannot lower that
position. A plan-hash change creates a new identity whose initial position is
independently `BeforeFirst`; active-contract queries expose it as building until
it catches up rather than pretending the old plan's frontier belongs to the new
semantics.

A deterministic aggregation overflow, malformed relevant event, missing commit,
plan mismatch, or integrity mismatch records the closed failure and enters
`Degraded` without advancing the affected frontier. An operator-requested rebuild
may recover a degradable condition. `Invalid` is reserved for a condition that
cannot be rebuilt from the available authoritative log and exact historical
plan. Repairing authoritative commits or fabricating projection rows is forbidden.

Generation exhaustion never wraps or resets and never fabricates a candidate.
An allocation attempt returns a closed `GenerationExhausted` control result and
leaves the exact prior control record unchanged. Thus an operator's failed
request to rebuild a healthy `Ready` projection does not suspend that published
generation. If exhaustion prevents recovery from an already `Degraded` state,
the existing failure remains durable and the projection may transition to
`Invalid` once the no-rebuild proof is established. Checked-size failure while
evaluating or applying a specific generation, or inability to retain its exact
historical plan, records the corresponding closed generation failure and fails
closed.

`GenerationExhausted` is a typed semantic transition result distinct from
`StorageErrorKind`, `ProjectionFailureCode`, and a public projection query
result. It is returned only after checked `highest + 1` fails before a write; it
never claims storage unavailability or uncertain commit.

### Storage API boundary

The storage semantic API exposes specialized synchronous operations for:

- reading one bounded apply snapshot for an identity, generation, and canonical
  set of group keys from one storage read transaction;
- querying one projection's control state, bounded published prefix rows, and
  lower continuation from one storage read transaction;
- reading one projection status plus the authoritative durable head from one
  storage read transaction;
- atomically applying one exact next sequence with its apply marker to one
  generation, or proving an equal marker for a duplicate sequence;
- atomically creating an initial candidate;
- atomically allocating a rebuild candidate; and
- atomically publishing a caught-up candidate or recording a closed failure.

These operations use owned values, explicit bounds, expected prior state, and a
closed result/error algebra. They expose no engine transaction, arbitrary
callback, raw table name, raw key/value write, plan evaluator, or async method.
Only projection-owned derived state uses these ports; authoritative command
commit does not depend on projection availability.

`ProjectionQuerySelector` is a checked, generation-neutral identity plus zero or
more complete leading group-component values. It is constructed against the
exact `BoundProjectionGroupSchema` and carries private validation evidence; an
engine adapter does not load a catalog or select a plan. Storage binds it to the
transaction-current published generation only after reading control. A
`ProjectionLowerContinuation` is internal, typed, and never client-authored. It
contains the exact identity, selected generation, complete generated prefix,
exclusive last returned complete key, and observed published frontier. All fields
are revalidated under the bound group schema. It is stored behind ADR-0007's
server-side public cursor token and is neither durable state nor an authorization
proof.

The semantic shapes include:

```text
read_projection_apply_snapshot(identity, generation, group_keys)
  -> { expected_frontier, rows: key -> Absent | Present(row, last_changed_sequence) }

query_projection(selector, limit, optional lower_continuation)
  -> Ready { generation, frontier, rows, optional next_lower_continuation }
   | Degraded { current, reason }
   | Invalid { reason }
   | ContinuationInvalidated

read_projection_status(identity)
  -> closed ProjectionStatus
```

The apply operation accepts the snapshot's expected frontier and per-row evidence
with the post-images and typed apply hash, then validates all three before writing.
Only published generations are publicly queryable; candidate generations are
visible solely through the internal apply-snapshot operation. There is no
preliminary control read: the one query operation reads control, handles a normal
uninitialized/unavailable lifecycle, derives the published-generation prefix,
scans, and returns its position and rows from one authoritative storage snapshot.
For continuation, the current published identity, generation, prefix, and
frontier must exactly equal the lower continuation fence. Publication or ordinary
frontier advancement invalidates the continuation and returns no rows. The
service maps that condition to its generic policy-safe invalid-cursor result; it
does not misreport the projection lifecycle as `Invalid`, silently restart a
page, or join two snapshots.

## Options Considered

1. **Lineage/ID/plan identity plus generation and framed canonical values:**
   Selected. It supports all accepted group scalars and rebuilds without frontier
   regression.
2. **Bare projection ID:** Rejected. IDs collide across lineages and do not
   identify changed plan semantics.
3. **Bundle hash or contract version in every key:** Rejected. It needlessly
   rebuilds an unchanged projection after unrelated compatible deployment.
4. **Reuse ADR-0016 key components:** Rejected. Decimal and money groups are
   accepted by ADR-0013 but excluded from that registry.
5. **Hash group values:** Rejected. It introduces collision semantics and loses
   exact prefix queries and reconstruction.
6. **Reset one generation's frontier during rebuild:** Rejected. It violates
   `PRJ-001` and can expose partially rebuilt state.
7. **Advance a global worker cursor separately from state:** Rejected. It can put
   the observable frontier ahead of durable derived rows.
8. **Use only the frontier as duplicate evidence:** Rejected. It proves that a
   sequence was applied but cannot compare a historical retry after later rows
   replace that sequence's post-images. The bounded apply hash freezes equality
   without retaining a second full row batch.
9. **Keep every retired frontier in one growing control record:** Rejected. It
   makes one hot record unbounded. Retired generations are inert and never reused;
   a later collector may remove one complete retired namespace under a separate
   policy.
10. **Read control, then scan rows in a second transaction:** Rejected. Publication
    or application between those reads can pair rows with the wrong generation or
    frontier. The generation-neutral selector and lower continuation keep each
    observation atomic and invalidate a continuation when its fence changes.

## Consequences

- A focused foundational follow-up must add the projection identity, generation,
  group/prefix/frontier/apply-key newtypes and key builders,
  `ProjectionApplyHash`, its domain primitive, and hash-domain fixtures before
  storage ports freeze.
- WP-040 freezes `ProjectionGroupSchema` and maximum-size diagnostics but does not
  persist projection rows.
- WP-060 depends on WP-040 and can then define the complete semantic projection
  port without depending on WP-170. WP-070 can implement the durable tables before
  its path ownership closes.
- WP-065 freezes durable projection messages after WP-060; WP-127 separately
  freezes public projection messages after WP-120. The two proto packages are
  hard-sequenced and remain the only writers of their respective schema phase.
- WP-170 implements only projection evaluation, lifecycle orchestration, waits,
  and queries over the already conformed storage port.
- Rebuild temporarily retains two generations and may consume up to twice the
  derived-state space. This is acceptable for the POC and must be observable.

## Compatibility

Identity fields and order, purpose/version bytes, component framing and codec,
apply-hash domain/payload, generation semantics, group schema, key and record
bounds, lifecycle/failure/reason tags, state/control/apply-marker record fields,
published-apply mode, prefix behavior, and atomic frontier rules are durable or
public semantic boundaries. Any incompatible change requires a new
key/record/hash version, fixtures, and a restartable idempotent migration or
controlled refusal.

Adding an unrelated contract declaration or deploying a bundle with the same
projection plan hash is compatible. Changing the projection plan hash creates a
new namespace and rebuild; it does not reinterpret old rows.

At proposal time, key prefixes `0x41`, `0x46`, and `0x47` do not collide with
the accepted entity `0x45`, conflict `0x43`, index `0x49`, or partition `0x50`
purpose bytes, and `riffdb.projection-apply/v1` is distinct from every accepted
hash domain. Acceptance adds all four entries to the central collision fixtures;
that check must remain exhaustive as registries grow.

## Security

Raw group keys and values are application data and are redacted from default
`Debug`, tracing, metrics, health, and public errors. Policy authorizes the exact
projection, partition/tenant context where defined, requested prefix, fields, and
row count before storage access. A hash or cursor is never an authorization proof.
Candidate and historical generations are not reachable through ordinary public
queries. MCP and gRPC call the shared service and never use projection storage
directly.

Bounds are checked before allocation and range construction. Malformed keys,
records, plans, events, cursors, and unsupported versions fail closed without
returning partial data.

## Testing

- Golden identity, frontier-key, complete group-key, and every prefix bytes,
  including decimal, money, variable-length, timestamp, date, UUID, and enum
  boundaries.
- Golden apply-marker keys and apply hashes for empty, absent-row, present-row,
  multiple-row, and maximum bounded batches; cross-domain hash inequality.
- Compiler snapshots for optional/collection group rejection, 1,024/1,025
  components, exact 4,096-byte maximum, and a 4,097-byte maximum.
- Schema-directed round trips and malformed decoder fuzzing for every length,
  tag, type, enum, currency, scale, generation, identity, and trailing-byte case.
- Equality and insertion-order properties; explicit tests that byte order is not
  advertised as numeric ordering.
- In-memory and redb conformance for exact/prefix scans, 500-row bounds, opaque
  pagination, wrong identity/generation, and cross-lineage/plan isolation.
- Conformance tests interleave application and queries and prove every returned
  row set, lifecycle, generation, and frontier comes from one storage snapshot;
  apply-snapshot restart tests reconstruct candidate aggregation from durable rows.
- Query tests cover a known identity with no control record, every unavailable
  lifecycle, status/head atomicity, and lower-continuation invalidation after
  frontier advancement or generation publication without returning a partial
  page.
- Reference-prefix application over every commit, including irrelevant events,
  multiple events for one group, multiple groups, duplicate submission, and gap
  rejection.
- Duplicate apply tests retry the current and historical sequences with equal and
  unequal post-images and prove equal-marker no-op versus fail-closed mismatch.
- Failpoints before row writes, between staged rows, before frontier update,
  during engine commit, and after durable commit prove old-or-new atomic state.
- Process-kill/reopen tests prove no frontier ahead of rows and no double
  aggregation.
- Every valid/invalid control-record shape and lifecycle edge, plus initial build,
  plan-change rebuild, same-plan shadow rebuild, failed rebuild, generation
  exhaustion, and atomic publication prove published-frontier monotonicity.
- Retirement tests prove replaced published and failed candidate generations can
  retain canonical rows/markers without an active frontier, can never be queried
  or resumed, and do not weaken marker-prefix checks on retained generations.
- Recovery tests fail published and candidate generations separately and prove a
  failed published generation stays suspended until replacement publication,
  while query results remain non-ready throughout recovery.
- Checked count/sum overflow degrades without advancing and never mutates
  authoritative state.
- Public service, gRPC, and MCP tests prove `after_sequence` never returns data
  behind the required commit, every non-ready lifecycle uses the exact closed
  result mapping, and unauthorized prefixes never reach storage.

## Requirements and Work Packages

- **Requirements:** `PRJ-001` through `PRJ-004`, `STO-002`, `STO-010` through
  `STO-012`, `STO-020` through `STO-022`, `REC-001`, `REC-003`, `API-001`,
  `MCP-030`, `MCP-031`, `POC-006`
- **Defines or blocks:** required ADR and focused foundational deliverable
  follow-up for `WP-010`; required ADR for `WP-040`, `WP-060`, formal durable
  schema package `WP-065`, `WP-070`, `WP-075`, `WP-120`, formal public schema
  package `WP-127`, `WP-130`, `WP-140`, `WP-170`, `WP-185`, `WP-190`, and
  `WP-200`
- **Final evidence:** `WP-190`, `WP-200`

## Decision Deadline

Exact acceptance is required before WP-040 freezes projection group schemas,
WP-060 merges projection persistence types, or WP-070 freezes redb projection
tables. ADR-0004 must be accepted first or in the same governance change so
`FrontierPosition`, formal WP-065, and the semantic storage boundary are
authoritative together.

Acceptance requires one companion governance reconciliation with these exact
metadata changes:

1. Add ADR-0017 to `required_adrs` for WP-010, WP-040, WP-060, WP-065, WP-070,
   WP-075, WP-120, WP-127, WP-130, WP-140, WP-170, WP-185, WP-190, and WP-200.
   WP-020 remains the completed phase-zero protocol/envelope baseline and is not
   retroactively reopened.
2. Extend WP-010 deliverables with `ProjectionIdentity`, `ProjectionGeneration`,
   the group/prefix/frontier/apply-key newtypes and builders,
   `ProjectionApplyHash`, the `riffdb.projection-apply/v1` registry entry, and
   golden/cross-domain fixtures. This foundational package owns no semantic apply
   request builder; WP-060 owns checked request construction and hashing. Mark the
   changes as a focused post-exit interface follow-up required before WP-040
   projection-schema fixtures and WP-060; existing paths and commands suffice.
3. Extend WP-040 deliverables with `ProjectionGroupSchema`,
   `BoundProjectionGroupSchema`, exact group/measure validation, complete key and
   stored-row maximum analysis, and deterministic fixtures. Add ADR-0017 to the
   reviewed IR-interface artifact.
4. Add WP-040 to WP-060 `depends_on`. Extend WP-060 with the exact semantic
   state/control/marker records, checked apply request/hash, apply and query
   snapshots, lower continuation, status read, lifecycle transitions, retirement
   behavior, and memory conformance tests; add `PRJ-001` through `PRJ-004` to its
   requirements. Existing WP-060 paths are sufficient.
5. Use formal WP-065, introduced by ADR-0004, for durable schema work. WP-065
   depends on WP-020 and WP-060, requires ADR-0017, and adds the versioned
   `riffdb.storage.v1` projection state, control, apply-marker, lifecycle/failure,
   and related conversion/descriptor/schema-hash/golden artifacts. Its existing
   formal proto-owner paths and generation commands are sufficient. WP-070 adds
   WP-065 as a hard dependency, requires ADR-0017, adds `PRJ-001` through
   `PRJ-004`, and persists only those reviewed records. WP-075 also requires
   ADR-0017 and the projection requirements because it runs the unchanged
   projection storage conformance suite.
6. Add formal WP-127, public API schema completion, after WP-020 and WP-120.
   It requires ADR-0006, ADR-0007, ADR-0009, ADR-0010, ADR-0012, ADR-0013, and
   ADR-0017 and owns the exact `riffdb.v1` projection query/status/frontier,
   lifecycle/failure, page/public-cursor, and unavailable-result messages,
   wire-structural validation, descriptors, schema hashes, and golden fixtures.
   WP-130, not WP-127 or `riffdb-proto`, owns checked conversion between these
   messages and API-neutral service types. WP-127's allowed
   paths are `Cargo.lock`, `proto/**`, `crates/riffdb-proto/**`,
   `fixtures/proto/**`, and `scripts/generate-proto*`; its acceptance commands are
   `cargo test -p riffdb-proto` and `./scripts/generate-proto --check` with a
   clean generated diff. WP-130 preserves its existing dependencies and adds
   WP-127 as a hard dependency. WP-127 is transitively after WP-065 through
   WP-120, so the two packages cannot edit shared proto paths concurrently
   without an unnecessary direct edge. WP-127's projection work maps `API-001`
   and `POC-006` to its
   conformance fixtures.
7. Reconcile SPEC Section 10.2 and the SPEC Appendix B storage-key examples with
   all three versioned projection key envelopes and identity/generation fields.
   Reconcile the storage integrity section so an absent frontier is
   `BeforeFirst`, retained generations require reciprocal contiguous markers, and
   retired generations follow the inert-data rule above. Reconcile SPEC Section
   15 with uninitialized state, exact control/lifecycle and public status/query
   mappings, explicit `FrontierPosition`, same-snapshot pagination, retirement,
   and marker-hash equality. Replace `PRJ-003` exactly
   with: "Projection application MUST be idempotent with equality validation by
   `(ProjectionIdentity, ProjectionGeneration, CommitSequence)`; a rebuild
   generation intentionally reapplies the authoritative prefix in a disjoint
   derived-state namespace." Scope `PRJ-001` to the published frontier of one
   exact `ProjectionIdentity`, while retaining the no-decrease publication rule.
8. Update the complete dependency metadata, not only one edge: add WP-040 to
   WP-060, add formal WP-065 and its WP-020/WP-060 inputs, add WP-065 to WP-070,
   add formal WP-127 and its WP-020/WP-120 inputs, and add WP-127 to
   WP-130. Add WP-065 and WP-127 to the P1 gate without removing an existing gate
   member. Apply the same graph to `work_packages.yaml`, SPEC Sections 19.4 and
   19.5, `diagrams/work_package_dag.dot`, and PLAN.md. Preserve every existing
   dependency. The parallelization waves must be recomputed so none of WP-040 and
   WP-060, WP-060 and WP-065, WP-065 and WP-070, WP-120 and WP-127, or WP-127 and
   WP-130 share a wave. Update SPEC Section 5.2 so `riffdb-storage-api` may depend
   on checked `riffdb-contract-ir` schemas but not on the compiler, projection
   evaluator, or a concrete engine; that direction is the reason for the new
   WP-040 to WP-060 edge.
9. Add ADR-0017 to the ADR index and SPEC Section 22 status table, update the SPEC
   version/revision history, and add amendment/clarification cross-references to
   accepted ADR-0010, ADR-0011, ADR-0013, and ADR-0016. No existing prefix or hash
   domain is reused. The accepted `PRJ-003` wording changes only through the exact
   companion amendment in item 7.

Merge order is the accepted ADR-0004/ADR-0017 governance change, foundational
WP-010 types and hash fixtures, WP-040 group schema, WP-060 semantic records and
ports, formal WP-065 durable Protobuf, and only then WP-070 redb persistence.
Later, WP-120 freezes the API-neutral public types, formal WP-127 adds their public
Protobuf mapping, and only then WP-130 implements gRPC. Exact durable and public
field numbers remain separately reviewed proto-owner artifacts; no implementation
may persist or expose an ad hoc or unversioned substitute while either review is
pending.
