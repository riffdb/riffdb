# ADR-0004: Semantic Storage API and Redb Baseline

- **Status:** Accepted
- **Direction approved:** 2026-07-12
- **Exact text accepted:** Yes; amended 2026-07-14, 2026-07-20, 2026-07-21,
  2026-07-22, and 2026-08-18 (Amendment 5: redb baseline 4.2.0)
- **Accepted:** 2026-07-13
- **Requires:** ADR-0007, ADR-0009, ADR-0012, and ADR-0017 accepted before or in
  the same governance change
- **Amends:** SPEC Sections 4.1, 5.2, 9.1, 9.5, 10.1, 13.4,
  17.3, 19.4, 19.5, 22.1, and 22.2; `WP-010`, `WP-050`, `WP-060`, `WP-070`,
  `WP-065`, `WP-080`, `WP-100`, `WP-160`, `WP-170`, `WP-190`, and `WP-200`
  metadata; gates; and the durable proto-owner work-package graph
- **Clarifies:** ADR-0005 pending admission and terminal atomicity
- **Implements the durable boundary of:** ADR-0006
- **Amended by:** ADR-0039 for V2 migration evidence, startup type state,
  catalog/invariant evaluation, and the narrow pre-sequence capacity result
- **Decision deadline:** Before WP-060 public traits or durable DTOs merge

The human maintainer accepted this exact bounded-snapshot and narrow-transaction
record on 2026-07-13 as part of the atomic semantic-interface governance batch.
On 2026-07-13 the maintainer also accepted the P1 readiness amendment below,
which separates redb structural evidence, catalog semantic validation, and
production activation without adding a storage-to-IR dependency.
On 2026-07-13 the maintainer accepted the schema-complete historical-key evidence
and dedicated ordered-commit-scan ceiling clarifications below.
On 2026-07-14 the maintainer accepted the exact pre-sequence candidate ordering,
mutation-affected epoch read, sequence-free write-plan, conservative encoded-
capacity reservation, and pre-stage canonical-envelope proof clarified below.
On 2026-07-14 the maintainer also accepted the companion durable-layout and
dependency amendment below: lineage-scoped bundle keys, one physical active-
catalog row, exact canonical entity/index/range keys, a standalone authoritative
event table with three-way reciprocity, the exact 26-record POC registry,
`sha2` 0.11.0 for backup manifests, and no Criterion dependency in WP-070.
On 2026-07-14 the maintainer accepted the single full-outcome terminal-row and
startup outcome/commit reciprocity clarification below.
On 2026-07-20 the maintainer accepted conservative checked-plan derived-index
bounds and the exact coordinator stop behavior for mutation-affected index-epoch
exhaustion below.
On 2026-07-21 the maintainer accepted the bounded lineage-overflow retention and
recheck rule plus explicit coordinator-durability construction below. These
clarifications add no durable record, wire field, or storage key.

## Context

The deterministic runtime must evaluate only owned values, while the commit
coordinator must validate transaction-current state and atomically persist one
complete command result. A live engine snapshot, storage transaction, callback,
or engine handle in runtime would couple command semantics to one backend and
could hold a writer transaction across evaluation. A generic key/value write API
would also permit application, catalog, capability, or transport code to bypass
the commit coordinator.

Accepted ADR-0003 requires every influential observation to be revalidated and
the exact historical commit-check plan to be re-evaluated. Accepted ADR-0005
requires durable pending admission and atomic terminal resolution. Accepted
ADR-0006 makes generated Protobuf and `StoredEnvelope` the encoding boundary but
deliberately defers semantic durable records until their Rust owner stabilizes.
The storage API therefore needs an exact semantic boundary without guessing
engine layout, transport DTOs, or later worker policy.

The original accepted candidate chain placed sequence assignment immediately
after private plan validation, while the original bounds text could be read to
require an exact aggregate encoded write-set calculation before transaction
open. That is impossible to state precisely when old index entries and affected
epoch positions are transaction-current inputs. The clarification below keeps
all input/runtime/component bounds before open, performs only the bounded exact
aggregate reservation after those current reads, and still forbids sequence
assignment until capacity is proven.

## Decision

### Ownership and dependency boundary

`riffdb-storage-api` owns:

- owned bounded snapshot, observation, dependency, validation-target,
  `EvaluatedCommand`, and pre-commit `CommitIntent` DTOs;
- structurally checked `ValidationReadRequest`, `TransactionCurrentState`,
  sequence-free `CommandWriteSetPlanV1`, `AtomicCommandRecordSet`,
  `ServiceAuditAppendIntentV1`, and
  `BootstrapServiceAuditStartV1` values;
- semantic durable DTOs and typed transition requests/results;
- the compound bootstrap transition that atomically appends its principal-less
  started audit before the authoritative bootstrap record set;
- synchronous engine-neutral read and consuming write-transaction traits;
- specialized catalog, capability, outbox, projection, administration,
  integrity, and backup persistence ports; and
- the closed storage error taxonomy and shared backend conformance contract.

`riffdb-contract-ir` owns the immutable checked bundle and command plan. It does
not depend on storage. Storage API may consume only ADR-0017's immutable checked
`ProjectionGroupSchema` and `BoundProjectionGroupSchema` value types in its
projection-schema module; this is the narrow reason WP-060 depends on WP-040.
It never consumes or interprets `CommandPlan`, executable expressions, compiler
services, or source diagnostics. `riffdb-runtime` constructs an
`EvaluatedCommand` and consumes a `ReadSnapshot`, but does not open storage or
receive admitted provenance claims. `riffdb-catalog` owns the opaque process-
local command `Ready`/resource evidence and pure current-recheck implementation
bound to its exact resolved plan/proof. `riffdb-commit` depends on catalog,
contract IR, and storage API; it consumes that opaque API and owns orchestration,
historical-plan lookup, semantic plan/target matching, validation, retry,
sequence assignment policy, and transaction progression. `riffdb-storage-memory` and
`riffdb-storage-redb` implement the same semantic interfaces. Generated Prost
messages and `StoredEnvelope` remain owned by `riffdb-proto`; they are encodings
of storage semantic DTOs and never replace them in runtime, service, or policy
APIs.

The dependency direction is deliberately one way. Arrows point from a dependency
to its consumer:

```text
riffdb-types / riffdb-errors --> riffdb-proto
riffdb-types / riffdb-errors --> riffdb-storage-api
riffdb-proto -----------------> riffdb-storage-api::proto_codec
riffdb-types -----------------> riffdb-contract-ir
riffdb-contract-ir -----------> riffdb-storage-api::projection_schema
riffdb-storage-api + riffdb-contract-ir --> riffdb-catalog
riffdb-storage-api -----------> riffdb-runtime
riffdb-storage-api + riffdb-contract-ir + riffdb-catalog --> riffdb-commit
```

Only the narrowly scoped `riffdb-storage-api::proto_codec` bridge may name
`riffdb-proto`. Semantic storage trait signatures and DTO constructors expose no
Prost type. `riffdb-proto` never depends on `riffdb-storage-api`; doing so would
reverse the foundational dependency, give the public protocol crate access to
storage traits, and contradict SPEC Section 5.2's `riffdb-proto` dependency
boundary. `riffdb-storage-api` owns checked semantic-record-to-wire mapping
because it owns the semantic DTO. `riffdb-proto` continues to own generated
messages, envelopes, descriptors, wire-structural validation, and its existing
foundational value/error conversion helpers.

Outside that one projection-schema value dependency, `riffdb-storage-api` does
not depend on `riffdb-contract-ir`; it never depends on `riffdb-runtime` or
`riffdb-commit`, and it never interprets a command plan. Its constructors
prove bounds, canonical order, target/key validity, record cross-links, and
other storage-structural facts only. `riffdb-commit` owns a private,
non-exported `CheckedCommitCandidate` that pairs one exact checked historical
plan with those structural values after semantic matching and transaction-current
re-evaluation. That proof never crosses a storage trait and is not claimed by
`CommitIntent`, `ValidationReadRequest`, or `AtomicCommandRecordSet`.

Rust visibility cannot grant a public trait operation to exactly one external
crate without adding a cycle or a false marker interface. Authority is therefore
enforced by both composition and architecture tests: only `riffdb-commit`
receives an authoritative write handle or may call its progression methods, and
no service, runtime, transport, catalog, policy, or worker crate may depend on
that handle. Structural constructors remain defensive even though they are not
semantic authorization proofs. A new shared "proof" crate or any broader
storage-to-IR dependency is explicitly rejected for v1. `riffdb-contract-ir`
must retain no reverse storage dependency.

No storage API type contains a redb/Fjall type, parser AST, source span, Tonic or
MCP type, asynchronous executor, clock, entropy source, credential, raw bearer
token, conflict-manager state, or reference into an input buffer.

### Reviewed redb dependency baseline

WP-070 may add `redb` 4.1.0 directly to `riffdb-storage-redb` with default
features disabled and no optional features. No other first-party crate may depend
on or re-export redb, and no redb type crosses the semantic storage API.

WP-070 may also add `sha2` 0.11.0 directly to `riffdb-storage-redb` with default
features disabled for SHA-256 backup-manifest checksums. It reuses the already
locked, reviewed pure-Rust version and MUST NOT enter command hashing, event
hashing, idempotency identity, or another semantic domain. A version, feature,
transitive graph, native/unsafe surface, license, or purpose change requires a
new dependency review. WP-070 uses a dependency-free repeated-run storage
benchmark harness and does not add Criterion.

The reviewed package is the current 4.1.x baseline, is licensed MIT OR
Apache-2.0, and declares Rust 1.89 as its minimum supported version. On the POC
Linux target it has no active normal dependency; `libc` is target-only for WASI.
It contains no native code. Its `build.rs` declares fuzz configuration and macOS
fuzz linker arguments only. The reviewed 4.1.0 source contains 37 localized
unsafe occurrences in roughly 21,000 source lines, concentrated in byte casts,
SIMD, and file I/O.

`redb` is not yet a dependency in the root `Cargo.lock`, so the current root
`cargo deny check` is not evidence for this graph. On 2026-07-13 an isolated
review manifest pinned exactly `redb = { version = "=4.1.0",
default-features = false }`, generated and retained its own lockfile, and ran:

```text
cargo deny --manifest-path /tmp/riffdb-redb-4.1.0-review/Cargo.toml \
  --locked check --config deny.toml advisories licenses bans sources
```

All four checks passed under the repository policy, and the isolated normal
Linux dependency tree contained only `redb` itself. This proves policy
eligibility for the reviewed candidate, not approval of a production lockfile.
WP-070 must add the exact dependency to the root workspace and root lockfile,
then pass the repository-root `cargo deny check` before merge; any resulting
graph difference stops for renewed human review.

This dependency review approves only the exact crate/version/features and
transitive graph. A version, feature, target-support, transitive-dependency,
build-script, unsafe-surface, or license change requires new human review. It
does not validate redb's durability or recovery claims: WP-070 must prove the
unchanged semantic conformance suite and process crash/reopen matrix. The exit
strategy remains the engine-neutral semantic API, memory reference
implementation, and isolated Fjall comparison; replacing redb cannot weaken the
contract.

### Frozen POC durable layout and record registry

The durable identity of a contract bundle is
`(ContractLineage, ContractVersion)`. Its canonical `contract_bundles` key is
`u32_be lineage_length || exact lineage UTF-8 || u64_be contract_version`.
The payload repeats lineage/version and carries the bundle hash; key/payload
mismatch is corruption. A version-only key is superseded and MUST NOT be
persisted.

`catalog_active` contains exactly one row under byte `0x01`. That row is the
sole physical representation of ADR-0019's active-contract metadata category;
the adapter MUST NOT duplicate it in `meta`. The exact `meta` keys remain
`format_version`, `database_id`, `next_application_sequence`,
`next_administration_sequence`, and `capability_bootstrap/v1`. Entity, secondary-
index, and index-epoch rows use the complete canonical `EntityKey`,
`IndexEntryKey`, and index-range-prefix bytes respectively, without prepending a
redundant owner already encoded in the key. Backend-private accelerators are
rebuildable memory state, not additional durable tables or unversioned values.

For a committed declared outcome, the `idempotency` table value is exactly one
full `StoredOutcomeV1` `StoredEnvelope` under the canonical identity. It is both
the terminal idempotency state and persisted outcome, not a pointer. The same
atomic command transaction deletes the matching pending row and writes the
outcome, commit, provenance, event, and outbox graph. Pending deletion writes no
envelope bytes or tombstone, and no separate outcome or second terminal envelope
exists. `StoredExecutionFailedV1` remains the disjoint terminal non-commit value
defined by ADR-0012.

`StoredDurableEventV1` is a separately registered ADR-0006 envelope in an
authoritative `events` table keyed by the exact 12-byte `EventId`. For every
committed event, the standalone row, the event nested in the matching outbox
intent, and the event nested in the commit record are exactly equal and commit
atomically. Missing, duplicate, orphaned, unequal, or hash-invalid copies are
corruption and are never repaired.

The POC compatibility registry contains exactly the 26 top-level payload types
listed in SPEC Section 10.3. In particular, its accepted fixed wire names include
`CapabilityRecordV1`, `CapabilityAdministrationAuditV1`,
`CapabilityBootstrapMarkerV1`, and `ServiceAuditRecordV1`; the catalog audit
payload is `StoredCatalogAdministrationV1`. `StoredReadDependenciesV1` and
capability grant/permission collections are nested closed messages rather than
top-level envelopes. WP-065 must freeze the accepted field/enum/oneof inventory,
schema hashes, bounds, and goldens without speculative reserved fields, record
types, key codecs, or ADR-0019 placeholders before WP-070 persists any record.

### Exact executable plan reference

Every admitted snapshot, pending record, intent, validation request, stored
outcome, commit record, and provenance record that refers to executable command
semantics uses one exact `ExecutablePlanRef`:

```text
ExecutablePlanRef
  contract_lineage: ContractLineage
  contract_version: ContractVersion
  contract_bundle_hash: ContractBundleHash
  command_id: CommandId
  command_plan_hash: PlanHash
```

All five fields must match one checked immutable historical bundle and command
plan. Resolving only the active version, only a command ID, or only a plan hash
is insufficient. The catalog may load the plan before opening the storage write
transaction because accepted bundles are immutable, but the coordinator checks
the complete reference again against the pending admission and intent. Missing
historical bytes, unsupported IR, bundle-hash mismatch, command mismatch, or
plan-hash mismatch fails closed; the current active plan is never substituted.

ADR-0005's pending-record list is clarified to contain this complete plan
reference. Contract lineage is already part of the idempotency identity;
contract version and command plan hash are already required by ADR-0005. The
bundle hash is an additive pre-release integrity field needed to make historical
lookup unambiguous. The accepted governance change amended ADR-0005's metadata
and body to cross-reference this complete `ExecutablePlanRef`, including
`ContractBundleHash`. ADR-0004 acceptance was conditional on that same-batch
amendment and does not otherwise rewrite ADR-0005 semantics.

### Owned bounded snapshot

`ReadSnapshot` is an owned value, not a trait object or live engine snapshot. A
synchronous storage call privately opens a consistent engine read view, copies
all requested records and epochs into bounded semantic values, closes every
engine handle, and only then returns the value to orchestration. Runtime cannot
extend an engine transaction lifetime by retaining a snapshot.

A `SnapshotRequest` contains the exact plan reference, binding targets in
ascending plan-local `BindingId` order, and canonical range targets. A binding
target is only an `EntityTypeId` plus complete validated `EntityKey`; binding
mode and declared failure outcome remain in the checked plan. Vector position
`n` corresponds exactly to dense `BindingId(n)`, avoiding a duplicate cross-crate
semantic ID type. Its storage-API constructor checks only bounds, canonical
structure, and key validity. Before storage reads begin, `riffdb-commit` matches
the complete request against the checked historical plan and retains that fact
in its private candidate state. Storage neither receives nor interprets the plan
to repeat that semantic check.

The accepted aggregate-root companion boundary adds root-validation targets in
ascending dense plan-local `RootValidationReadId` order. They are separate from
source binding targets because they have no binding mode or business outcome.
Each target is an `EntityTypeId` plus complete validated `EntityKey`. The
request constructor applies the same bounded structural checks; the coordinator
alone proves that each target is the result of the exact checked plan derivation.

The returned snapshot contains:

```text
ReadSnapshot
  plan: ExecutablePlanRef
  observed_through: optional CommitSequence
  bindings: binding observations in exact BindingId order
  root_validations: root observations in exact RootValidationReadId order
  ranges: range observations in canonical target order
  read_dependencies: canonical, duplicate-free dependencies
```

`observed_through` is absent for an empty application log. It describes the
consistent authoritative application prefix seen by the snapshot; it is not a
lease, validation proof, or substitute for per-record dependencies.

Each binding observation repeats the requested entity identity and is exactly
one of:

- `Absent`; or
- `Present`, carrying nonzero `EntityVersion`, the writing contract version,
  and the complete canonical entity field record needed to preserve fields not
  changed by the command.

The snapshot materializer never chooses a declared business outcome. Runtime
applies the accepted binding rules in ascending `BindingId`: absent read/mutate
and present create observations select the plan's declared first binding
failure. Storage corruption or failure is never converted into that outcome.
Only after every source binding succeeds does runtime inspect root-validation
observations. An absent required root is an integrity fault, not a source binding
failure or application outcome. An identical source and root target may be read
once physically, but both ordered semantic observations remain present.

An index-range observation contains the exact `IndexId`, a validated
component-complete prefix no larger than the durable-key bound, its
`IndexEpochPosition`, and ordered bounded entries from the same read view.
Prefixes and entries use
ADR-0016 key schemas. If command IR does not expose an accepted bounded indexed
read plan, the compiler rejects a write-influencing indexed read; the storage
port's existence does not enable a hidden scan.

Snapshot observations are immutable and do not claim freshness. After the
snapshot is returned, correctness comes only from canonical dependencies and
transaction-current validation.

The storage-owned `ReadSnapshot` is the bounded raw physical observation before
catalog-proof-guided lineage normalization. `riffdb-catalog` owns an opaque
process-local materialization API bound to the exact resolved plan. Its successful
`Ready` evidence supplies the bounded normalized snapshot plus opaque current-
recheck evidence. A valid null expansion beyond the shared 16 MiB ceiling
instead returns resource evidence retaining only this original raw snapshot plus
the exact resolved plan/proof and no over-budget normalized data or masks. The
raw snapshot remains bounded by the existing storage limit; the resolved
plan/proof retain their independent accepted bounds. Evidence never crosses a
storage trait or becomes a new snapshot, intent, or durable-record variant.

The existing execution-failure transaction does not trust dependency versions
alone for this overflow case. The coordinator first compares every canonical
dependency, then invokes the resource evidence's catalog-owned pure current-
recheck operation with transaction-current raw binding/root observations. That
operation proves exact raw physical equality and deterministically repeats the
same normalization and exact charge. The storage transition may
replace Pending with `StoredExecutionFailedV1` carrying `ResourceLimit` only
when that deterministic pass reproduces the same valid overflow. A dependency change
reevaluates; raw drift or a nonmatching normalization result is integrity. The
operation performs no catalog lookup, storage I/O, runtime evaluation, clock, or
entropy. The storage API remains IR-blind and stores no overflow evidence, mask,
or normalized snapshot.

### Canonical read dependencies

The v1 closed dependency registry is:

```text
0x01 EntityObservation
  entity_type + complete EntityKey
  expected = Absent | Present(nonzero EntityVersion)

0x02 IndexRangeEpoch
  IndexId + validated complete-component prefix + IndexEpochPosition
```

Every binding observation produces one entity dependency, including observed
absence for a create or missing read. Every influential range produces one
range-epoch dependency. Dependencies are sorted by tag and exact target bytes,
duplicate targets are collapsed only when their observations are identical, and
conflicting duplicate observations are an integrity failure. Encodings use
fixed-width big-endian IDs and `u32` big-endian lengths for variable key/prefix
bytes. Zero or unknown tags, noncanonical order, duplicate entries, invalid
keys, or trailing bytes reject.

Every present or absent root-validation observation also produces the existing
`0x01 EntityObservation` dependency. Identical source/root entity targets
therefore collapse to one dependency only when their observations agree. This
does not collapse their distinct semantic positions in `ReadSnapshot`.

Influential range dependencies and mutation-affected epoch targets are separate
canonical sets. An influential `IndexRangeEpoch` records a snapshot range whose
entries affected evaluation and must compare equal during dependency validation.
A mutation-affected target is derived later, inside the write transaction, from
the normalized transaction-current old index entries and proposed new entries
whose presence or covered value changes. Its position is read so the command can
stage exactly one advance for that affected prefix. An exact prefix may occur in
both sets, but neither set is derived from or required to equal the other. The
coordinator and storage API use purpose-specific checked collection types so a
mutation-affected set cannot silently stand in for influential read evidence.

#### Exact `IndexRangeEpoch` bucket semantics

One range bucket is identified by the exact validated scan-prefix bytes from
ADR-0016: the six-byte index envelope `0x49 0x01 | IndexId:u32_be`, followed by
zero or more complete leading index-component payloads, and never any part of a
component or the entity-key suffix. The zero-component six-byte prefix is the
whole-index bucket. The semantic target repeats `IndexId`; construction verifies
that it equals the ID embedded in the prefix. The physical `index_epochs` key is
the exact prefix bytes, so there is no second backend-specific bucket encoding.

For an index with `m` components, one visible index entry belongs to exactly
`m + 1` buckets: the whole-index bucket and the bucket after each complete
leading component. After private transaction-current validation and before
capacity reservation, the coordinator normalizes the transaction-current and
proposed index entries and identifies every entry whose presence or covered
value changes. The affected-bucket set is the exact union of all buckets for
those old and new entries. The transaction reads every affected current epoch
position before freezing the sequence-free write plan. Each affected bucket
advances exactly once for that staged command, regardless of how many changed
entries named it; unchanged net entries do not advance it. A later command in
the same write batch observes those staged advances and may advance the same
bucket once again.

Epoch state is represented semantically as the closed
`IndexEpochPosition::{BeforeFirst, Value(nonzero IndexEpoch)}`. An absent
physical epoch record means `BeforeFirst`; no prefix catalog is precreated. The
first affected command stores `Value(1)`. A stored zero epoch is noncanonical,
and advancing `u64::MAX` fails the complete transaction with
`SequenceExhausted`; no epoch wraps. Epoch updates are part of the same atomic
command record set as the corresponding index changes.

Every range observation records one exact prefix and the epoch read in the same
view as its bounded ordered entries. The command transaction rereads that exact
bucket and requires equality before commit-plan evaluation. Because every
insert, delete, entity-key replacement, and covered-value replacement advances
all affected leading-prefix buckets, equality excludes phantoms or changed
covered values for that range. Component-incomplete prefixes reject. This
mechanism must have backend-conformance tests even while the compiler rejects
write-influencing indexed reads; its existence does not silently enable such an
IR operation.

Checked grammar/IR-v1 command-plan construction conservatively proves before
hashing that a successful mutation cannot require more than 4,096 index-entry
mutations, 4,096 mutation-affected prefix targets, 4,096 combined binding,
root-validation, and affected-prefix validation positions, or 16 MiB for the
affected target/current-epoch state. It uses declared bindings, assigned fields,
index component order, and component maximum payload sizes with checked
arithmetic. It may reject a plan whose runtime keys could happen to deduplicate
across bindings; this is an accepted compiler-specific lower limit. The exact
transaction path retains incremental checks for every count and byte ceiling.
Reaching one from a canonical checked plan is an internal invariant defect, not
a command-visible resource-limit terminal.

The illustrative `ReadDependency::Predicate { captured_values }` in SPEC
Section 9.4 is not a sufficient commit proof and is not a v1 storage dependency.
Captured booleans or values can be stale. Predicate and invariant correctness is
represented by the exact plan reference, structurally checked validation
request, and coordinator-private semantic match and is re-evaluated over
transaction-current values.

### Structural validation requests and private semantic proof

Before opening a write transaction, the coordinator resolves and validates the
exact historical plan. It checks that:

1. binding targets have exactly the plan's count, order, entity types, key
   schemas, and derived keys;
2. root-validation targets have exactly the plan's count, order, entity types,
   key schemas, and derived keys;
3. every influential binding/root/range has exactly one matching canonical read
   dependency;
4. mutation, event, outcome, partition, and validation-target shapes are allowed
   by that plan; and
5. no undeclared target, dependency, mutation, event, or predicate is present.

After this check, the coordinator constructs a storage-owned
`ValidationReadRequest` containing the plan reference, plan-ordered entity
source-binding targets, plan-ordered root-validation targets, and canonical
range targets. Its constructor rechecks bounds, canonical order, and key
structure but does not claim that the targets match a command plan. The
coordinator retains that semantic match in its private candidate state. The
request contains no checked plan, executable closure, captured predicate result,
or public semantic-proof marker.

Inside the short write transaction, storage returns transaction-current complete
entity observations and influential range epochs as `TransactionCurrentState`
for all requested targets. The coordinator:

1. compares every current absence/version/epoch with the intent's dependency;
2. evaluates the exact historical compiler-produced commit-check plan against
   transaction-current values plus the proposed complete post-images; and
3. verifies all mutation preconditions and normalizes the exact old/new index
   deltas against that same state; and
4. derives the canonical mutation-affected prefix-target set from those exact
   old/new index entries.

Only successful completion of those steps creates the private
`CheckedCommitCandidate`. The transaction then reads the exact current positions
of every mutation-affected target. The coordinator combines the retained intent,
affected targets/positions, normalized index changes, epoch advances, semantic
charge, and WP-065-defined conservative encoded upper bounds into one exact
sequence-free `CommandWriteSetPlanV1`. Storage verifies that this plan matches
the candidate values retained by the consuming state and reserves its semantic
and conservative encoded aggregate capacity before any sequence is assigned.

After reservation, the coordinator may request the sequence and construct the
complete graph. The storage-owned `AtomicCommandRecordSet` constructor checks
complete membership, cross-links, canonical order, bounds, structural sequence
consistency, and exact equality with the retained intent, sequence-free plan,
and assignment. WP-065 defines and proves the real canonical `StoredEnvelope`
upper bounds; before durable redb staging, WP-070 canonically encodes every final
envelope, computes the actual byte charge for each record class, and proves it
does not exceed the corresponding retained upper bound. WP-060 memory
conformance uses explicit synthetic charges and cannot depend on the WP-065
codec. Storage still does not assert that the record set is faithful to a
`CommandPlan`; the private coordinator proof and first-party dependency ban
provide that guarantee without reversing a crate dependency.

A changed dependency, false validation plan, or mutation precondition mismatch
invalidates the intent and produces no write or sequence. The coordinator may
perform a bounded full command retry under the checked plan's retry policy; it
must not patch the old intent, trust the captured outcome, or expose current
values in an error. Missing evidence or an impossible plan/target mismatch is an
integrity defect, not a normal retry.

### Evaluated command and pre-commit intent boundary

The deterministic runtime returns a storage-API-owned `EvaluatedCommand`. It is
an owned, self-contained, canonically ordered value containing only:

- the complete `ExecutablePlanRef` used for evaluation;
- structurally checked binding/range targets and canonical read dependencies;
- create/replace entity mutation intents with complete canonical post-images and
  expected absence/version;
- ordered pre-commit event intents with stable event type and canonical payload;
  and
- the encoded declared business outcome.

It contains no request or idempotency identity, actor, provenance claim, current
capability, logical time, partition/conflict hash, sequence, durable record, or
orchestration proof. A declared zero-mutation rejection still returns an
`EvaluatedCommand` so ADR-0005 can assign one sequence and persist its terminal
business outcome.

After evaluation, `riffdb-commit` constructs the final storage-API-owned
`CommitIntent` from the exact stored pending admission, the acquired plan-derived
partition/conflict evidence, and the `EvaluatedCommand`. Its checked constructor
requires the plan reference and all structural targets to agree and combines:

- the pending admission identity, original admission request ID, canonical input
  hash, complete `ExecutablePlanRef`, fixed logical time, admitted actor, and
  ADR-0007 stored admitted-provenance snapshot;
- the validated partition identity and bounded conflict/partition hashes; and
- every field of the exact `EvaluatedCommand` above.

This assembly step does not re-evaluate the command or permit the coordinator to
edit its mutations, events, or outcome. It keeps provenance outside deterministic
runtime while leaving the final `CommitIntent` fully self-contained as required
by `TXN-030`.

Mutation targets and dependencies use canonical target-byte order. Duplicate
mutation targets reject. Event intents preserve deterministic instruction
occurrence order because that order becomes the zero-based event ordinal.
Records and fields inside every value remain in their accepted canonical order.

Neither an evaluated command nor an intent contains a `CommitSequence`,
`EventId`, new entity version,
durability result, replay flag, final outbox entry, persisted outcome, provenance
record, commit record, serialized `StoredEnvelope`, transaction handle, lease,
or checked-validation success claim. Those values exist only after
transaction-current validation and coordinator sequence assignment.

### Semantic durable DTOs and Protobuf encoding

`riffdb-storage-api` owns the semantic Rust records for metadata, entity state,
pending/terminal idempotency, stored outcomes, commit records, durable events,
outbox intent/status, provenance, catalog state, capabilities, administration
audit including ADR-0007's `StoredServiceAuditRecordV1`, projection
state/frontier, and typed integrity results. Fields are private outside checked
constructors and canonical decoders. Rust layout, enum
discriminants, insertion order, and `Debug` output never define durable bytes.

ADR-0006 remains the encoding authority. After a semantic record stabilizes, a
small proto-owner interface PR adds its `riffdb.storage.v1` message, descriptor,
schema hash, golden bytes, wire-structural validation, historical compatibility
registration, and checked mapping in the storage API's narrow `proto_codec`
bridge. WP-070 may not persist a semantic DTO until that record's schema and
mapping are reviewed. Storage traits expose semantic DTOs, not Prost messages or
raw envelope payloads.

Acceptance of this ADR does not authorize a later record field, Protobuf message,
or generated artifact. Before a semantic record first becomes durable, the work
package manifest must assign one focused proto-owner change with allowed paths
`proto/**`, `crates/riffdb-proto/**`, `fixtures/proto/**`,
`scripts/generate-proto*`, the exact
`crates/riffdb-storage-api/src/proto_codec/**` bridge plus
`crates/riffdb-storage-api/src/lib.rs` and
`crates/riffdb-storage-api/Cargo.toml`, and `Cargo.lock` when dependency
resolution actually changes.
The current WP-060 and WP-070 path sets do not grant that authority.
The proto-owner change must merge before the corresponding redb persistence
change and must add the ADR-0006 descriptor, schema hash, golden bytes, decoder
bounds, and historical compatibility registration.

In particular, `riffdb.storage.v1.ServiceAuditRecordV1` is a proto-owner
follow-up to the accepted ADR-0007 semantic record. Neither WP-060 nor WP-070
may invent its fields. The storage interface may freeze the bounded
`ServiceAuditAppendIntentV1` and `StoredServiceAuditRecordV1` Rust shapes only
after ADR-0007 is accepted; redb persistence follows their reviewed durable
encoding.

Durable decoding validates the ADR-0006 envelope, record registry, schema hash,
canonical Protobuf bytes, semantic bounds, cross-record references, and semantic
constructor before returning a record. Unknown or historical payloads are not
decoded and rewritten implicitly. This ADR does not invent the deferred field
numbers that ADR-0006 assigns to later proto-owner PRs.

### Synchronous consuming transaction protocol

All engine methods are synchronous. The asynchronous boundary is the bounded
coordinator queue outside storage; a dedicated blocking coordinator context
drives storage calls. A synchronous storage transaction is never sent to an
async task, held across `.await`, or exposed through a callback.

The command transaction is a consuming typed state protocol with two batch
classes and a candidate chain parameterized by its prior batch class. This is
expressible on stable Rust without generic-const arithmetic:

```text
EmptyBatch
  -> CandidateAdmission<EmptyBatch>
  -> CandidateStateRead<EmptyBatch>
  -> CandidateAwaitingValidation<EmptyBatch>
  -> CandidateAffectedEpochRead<EmptyBatch>
  -> CandidateAwaitingCapacity<EmptyBatch>
  -> CandidateCapacityReserved<EmptyBatch>
  -> CandidateSequenceAssigned<EmptyBatch>
  -> NonEmptyBatch { staged_count = 1 }

NonEmptyBatch { staged_count: NonZeroU8, staged_bytes }
  -> CandidateAdmission<NonEmptyBatch>
  -> CandidateStateRead<NonEmptyBatch>
  -> CandidateAwaitingValidation<NonEmptyBatch>
  -> CandidateAffectedEpochRead<NonEmptyBatch>
  -> CandidateAwaitingCapacity<NonEmptyBatch>
  -> CandidateCapacityReserved<NonEmptyBatch>
  -> CandidateSequenceAssigned<NonEmptyBatch>
  -> NonEmptyBatch { staged_count = prior + 1 }

NonEmptyBatch -> CommittedBatch
```

`EmptyBatch` has no `commit` operation. `NonEmptyBatch` carries the checked
runtime count and aggregate encoded-byte budget. Starting another candidate is
count-only and requires only `staged_count < 64`; it cannot know or reserve the
candidate's exact aggregate charge until transaction-current values and affected
epochs have been read. Each arrow
consumes the prior wrapper and returns the next wrapper or a closed typed result.
Terminal replay, input mismatch, missing pending state, dependency change,
validation rejection, or another nonfatal candidate resolution returns the
exact unchanged prior batch class; that candidate receives no sequence and
stages no write. An empty prior batch may then close and roll back. A nonempty
prior batch may commit its staged prefix and retry or return the unresolved
candidate separately. A storage, corruption, incompatibility, invariant, or
structural record-set failure aborts the entire uncommitted batch instead of
returning the prior batch.

`CandidateAwaitingValidation<Prior>` may advance only after the coordinator
creates its private `CheckedCommitCandidate` and supplies the exact canonical
mutation-affected prefix set; storage neither constructs nor interprets that
proof. `CandidateAffectedEpochRead<Prior>` reads those positions in the same
transaction. `CandidateAwaitingCapacity<Prior>` accepts only the exact
sequence-free `CommandWriteSetPlanV1` matching the retained intent, targets, and
positions. `CandidateCapacityReserved<Prior>` exists only after semantic and
conservative encoded aggregate capacity is retained, and it alone may assign the
next sequence. The type state enforces call order, while the composition boundary
enforces that only the coordinator can assert completion of its private semantic
proof; no storage state is itself that proof.

After sequence assignment, constructing and verifying the complete graph and
proving actual canonical per-class `StoredEnvelope` charges fit the retained
upper bounds are mandatory before staging. Staging that complete atomic record
set is the only transition from either candidate chain to `NonEmptyBatch`.
Dropping any uncommitted state rolls back the entire batch; rollback is
idempotent. `commit` consumes only `NonEmptyBatch` and is the only operation that
can report durable success. A candidate state can neither commit nor be reused
to start another candidate.

The interface has no function accepting a closure, trait object callback,
arbitrary table/key/value, untyped batch, engine transaction, or caller-selected
sequence. It exposes no savepoint, nested transaction, generic compare-and-swap,
or mutation method callable from transports or runtime. Backend-private backup,
compaction, integrity, and statistics handles never cross the adapter crate.

### Coordinator order and atomic command record set

The coordinator performs the following exact order for each mutating command:

1. Before runtime, use a separate short typed admission transaction to look up
   the bounded ADR-0005 digest candidates, reject multiple matches, replay an
   equal terminal result, reject different input, resume an equal pending record,
   or atomically create one pending record with no sequence.
2. Resolve the pending record's complete historical plan, acquire the declared
   mutation capability, recheck the pending record, materialize the owned
   snapshot, and run deterministic evaluation with no storage transaction open.
3. Check every input, runtime-result, collection, component, key, and semantic
   byte bound that can be known without transaction-current state, then open a
   command write transaction and start a count-only candidate.
4. Recheck that the exact pending identity, input hash, original admission data,
   and `ExecutablePlanRef` still match and that no terminal result exists.
5. Read the complete influential `ValidationReadRequest` into one
   `TransactionCurrentState` from transaction-current state.
6. Compare every absence/version/epoch dependency and re-run the exact historical
   commit-check plan over current values plus proposed post-images; verify
   mutation preconditions and normalize exact old/new index changes.
7. Derive the canonical mutation-affected whole-index/leading-prefix target set
   from those normalized changes, then read each affected epoch position in the
   same transaction.
8. Freeze the exact sequence-free `CommandWriteSetPlanV1`, including all index
   mutations and epoch advances, and reserve its semantic charge plus WP-065's
   conservative per-class and aggregate canonical-envelope upper bounds. A
   capacity failure returns the unchanged prior batch and assigns no sequence.
9. Only after reservation succeeds, request the next application sequence from
   transaction metadata; the coordinator is the sole semantic caller of this
   transition.
10. Construct final entity versions, event IDs and hashes, durable event records,
    outbox intents, stored outcome, provenance, commit record, and allocator
    metadata from the retained intent/write plan and exact assigned sequence.
11. Construct one structurally checked `AtomicCommandRecordSet`, verify its exact
    retained intent/plan/assignment equality, canonically encode every final
    envelope, and prove every actual per-class byte charge is within its retained
    upper bound before staging any part of the graph.
12. Stage the complete set and commit using the configured durability mode.
13. Only after durable success, return the committed result and publish
    notifications. Replay metadata is created for the current response and is
    never persisted as another result.

For one command, the atomic application record set is exactly:

- next application-sequence metadata;
- complete entity creates/replacements with monotonic nonzero versions;
- corresponding secondary-index removals/additions and affected prefix epochs;
- resolution/removal of the pending state and one terminal idempotency record;
- the original typed stored outcome and durability mode;
- every durable event and matching pending outbox intent with stable `EventId`;
- one policy-approved provenance record; and
- one complete commit record, including complete mutation post-images.

No-op declared business rejection omits entity/index/event/outbox entries but
still atomically writes its sequence metadata, terminal idempotency state, stored
outcome, provenance, and commit record. Projection state and delivery status are
not in this authoritative command transaction. A failure before durable commit
leaves the prior pending admission visible and no terminal partial record.

### Sequence and empty-frontier representation

The first assigned application `CommitSequence` is 1. The first assigned
`AdministrationSequence` is 1. Zero is reserved/unassigned and is invalid in an
application commit, event ID, entity provenance reference, administration audit
record, or durable ordered key. The semantic allocator state is closed:
`Next(nonzero sequence)` or `Exhausted`. Empty database metadata stores
`Next(1)` and no last-assigned sequence. Allocating `Next(N)` assigns `N` and
atomically stores `Next(N + 1)` when checked addition succeeds, or `Exhausted`
when `N == u64::MAX`. An operation needing multiple consecutive values verifies
the complete range before assigning any; bootstrap therefore cannot begin from
`Next(u64::MAX)`. `Exhausted` rejects without a write. Allocation never wraps,
reuses a value, or makes a sequence visible outside its complete atomic
transaction. The durable encoding of this semantic state remains a WP-065
proto-owner review item.

An empty projection frontier is not a committed sequence. Semantic storage uses:

```text
FrontierPosition
  BeforeFirst
  AppliedThrough(nonzero CommitSequence)
```

A Protobuf mapping may encode `BeforeFirst` as explicit position tag or a
documented zero sentinel, but generated code must convert it to
`FrontierPosition` before semantic use. `CommitSequence(0)` is never passed as
an application commit or used to create `EventId`. This clarifies the
illustrative `ProjectionFrontier.applied_through: CommitSequence` in SPEC 15.2
without changing ADR-0010's contiguous-prefix rule.

Administration operations use their separate sequence space and never allocate
an application sequence unless a future accepted ADR changes the model.

This proposal intentionally tightens the current pre-release WP-010 scaffold,
whose generic numeric constructors still admit zero. Before WP-060 starts, one
manifest-authorized foundational interface follow-up must make
`CommitSequence`, `AdministrationSequence`, `EntityVersion`, and `IndexEpoch`
nonzero by construction over `NonZeroU64`. Each exposes `new(u64) -> Option<Self>`,
`first()`, `get()`, and `checked_next()`; fallible `TryFrom<u64>` replaces
infallible `From<u64>`. Durable byte decoders are fallible and reject zero.
`EventId::new` remains infallible because its `CommitSequence` is then nonzero by
construction, while `EventId::from_be_bytes` becomes fallible. The follow-up also
places `FrontierPosition::{BeforeFirst, AppliedThrough(CommitSequence)}` in
`riffdb-types`; storage, projection, and service reuse that one type rather than
declaring copies. `riffdb-storage-api` owns the analogous
`IndexEpochPosition::{BeforeFirst, Value(IndexEpoch)}` because it is a storage
range-validation state. Rust representation is not a durable encoding.

### Specialized persistence ports

The storage API is split into capability-specific traits or handles rather than
one broad interface:

- **Authoritative entity/commit reads:** get one entity, bounded validated index
  scan, get one stored outcome/commit/provenance, and ordered commit scan after
  an optional position. Absence is data, not an error.
- **Catalog:** immutable bundle put/get, active-catalog read, and one typed
  expected-version activation operation. Bundle persistence, active pointer,
  administration audit, and administration sequence are atomic. Notification is
  post-durability and outside storage. The checked activation request carries the
  one canonical timestamp obtained by the coordinator from its
  `AdministrationClock`; storage never samples time.
- **Capability:** digest lookup and ID lookup plus typed bootstrap/create/revoke
  administration operations. The stable record, lookup cross-link, bootstrap
  marker, lifecycle transition, audit, and sequence must follow ADR-0009 after
  its exact text is accepted. New bootstrap is the one compound typed operation
  that writes a principal-less service-audit `started` record immediately before
  its capability-administration record in the same transaction. Its request
  carries one checked `AdministrationClock` timestamp for both records. Normal
  create/revoke instead carries the exact transaction-current
  `AuthorizationClock` value already accepted by the verifier as both transition
  and capability-administration timestamp. No raw token enters storage, and
  storage samples neither clock.
- **Outbox:** bounded ordered pending scan and typed claim/renew/succeed/retry
  transitions keyed by `EventId`. Initial event/outbox intent is authoritative
  and command-atomic; delivery attempts and status are worker state and never
  reconstruct a missing intent. Absence of a status row is the canonical initial
  `Pending` delivery state, not evidence that the authoritative intent is absent.
- **Projection:** ordered commit input plus one typed atomic apply operation that
  applies all relevant effects for one next sequence, records idempotency,
  updates derived rows, and advances `FrontierPosition` together. Gaps and
  plan/identity mismatch reject; authoritative records are never repaired from a
  projection.
- **Administration:** typed catalog/capability/bootstrap operations, the exact
  standalone service-audit append below, and ordered audit reads. It is not a
  generic administrative mutation surface.
- **Operations:** bounded integrity report, authoritative-readiness validation,
  separate outbox/projection subsystem health, and offline backup/restore
  primitives. No generic repair is exposed. Authoritative corruption or
  incompatibility fails core readiness; a finding confined to rebuildable worker
  state degrades only its owning subsystem until its typed recovery owner acts.

Authoritative catalog and capability transitions are driven only by the commit
coordinator. Outbox and projection workers may drive only their specialized
non-authoritative state transitions. gRPC, MCP, CLI, SDK, service, compiler, and
runtime crates never receive one of these persistence ports directly as a way to
bypass the shared service/coordinator boundaries.

#### Exclusive structural evidence and composed readiness

Opening a production database is a type-state progression, not a boolean set by
the redb adapter. The source-free database-identity probe runs first. If it
returns `NeedsInitialization`, the commit-owned
`DatabaseInitializationExecutor` must complete the accepted atomic initialization
transition before any evidence session begins. If it returns an existing
identity, that exact durable `DatabaseId` is used. Partial or malformed metadata
fails closed and never enters either path.

After initialization, `riffdb-storage-redb` opens one exclusive
`StructuralEvidenceSession`. The session owns and withholds every dormant
authoritative read and mutation port, prevents any interleaving authoritative
mutation, and is bound to one process-local session identity and the durable
`DatabaseId`. Neither general service reads nor command execution can observe
the database while this session is live. Dropping or failing the session drops
the entire open attempt; it never exposes a partially validated backend.

The session performs the complete bounded structural pass and exposes only
session-scoped, bounded reads of stored historical bundle bytes and their
storage-structural metadata. The pass must reach an exact end marker for every
authoritative namespace. A page limit, finding limit, or diagnostic truncation
is never treated as evidence that unvisited rows are valid. The storage-owned
`HistoricalSemanticEvidence` records prove only decoding, canonical encoding,
bounds, keys, sequence continuity, record reciprocity, durable references, and
the other storage-structural facts in this ADR. They contain no `CommandPlan`,
IR semantic proof, parser value, or executable expression.

The pass enumerates every persisted entity key, complete index-entry key, and
persisted range-prefix key in bounded, exact-end evidence streams. Each evidence
item carries only canonical key bytes and the IR-opaque durable owner/reference
facts needed for the catalog to select the exact retained or active ADR-0016
`KeySchema`; it carries no decoded schema or IR. Storage validates framing,
bounds, canonical byte preservation, ownership cross-links, and record
reciprocity, but cannot claim component completeness because ADR-0016 key bytes
contain no component type tags. While the same session remains exclusive,
`riffdb-catalog` must validate every enumerated key against its selected exact
historical schema. Unknown or mismatched ownership, a missing schema, incomplete
components, omitted evidence, or failure to consume exact end prevents
`ValidatedCatalogHistory` and fails the open. This evidence requirement changes
no durable key encoding and creates no storage-to-IR dependency.

The structural pass receives three already checked, value-only startup inputs:
one canonical value sampled and validated through the policy-owned
`AuthorizationClock`, one typed inventory of readable capability digest schemes
and key IDs, and one independently typed inventory of readable idempotency
digest schemes and key IDs. The inventories contain no raw key material or
provider handle. They exist only so the pass can enforce ADR-0009's live
capability and ADR-0005's retained-idempotency support rules. Storage samples no
clock, opens no secret provider, and performs no authorization decision.

While the session remains exclusive, `riffdb-catalog` borrows its bounded
historical-bundle reader. WP-050 resolves and IR-validates every stored bundle,
its immutable identity/hash/version relations, every referenced historical
plan, and the active-catalog relation, and consumes the exact end marker. On
success it returns an opaque, privately constructible, nonserializable
`ValidatedCatalogHistory` bound to the same process-local session
identity and `DatabaseId`. That value is catalog-owned and never crosses a
storage trait. The storage API and redb adapter neither depend on catalog/IR nor
interpret the proof.

Only after both complete passes succeed may the evidence session finish and
yield `StructurallyOpened` dormant ports. WP-130 mechanically consumes those
ports together with the matching `ValidatedCatalogHistory`; a session or
database mismatch fails closed. That composition step is the only operation that
activates the production catalog, commit, authorization, service, and gRPC graph
and establishes composed authoritative integrity for the runnable P1 server. It
does not by itself report general readiness: WP-130 must also enforce ADR-0007's
bootstrap/deployment lifecycle, require a valid active catalog, and require both
authoritative allocators to be able to progress. It stores no ready bit and
persists no validation proof. WP-070 therefore proves structural completeness
and produces structural evidence; it cannot by itself claim full authoritative
readiness.

This startup proof does not replace per-command validation. Every command still
resolves its exact immutable historical bundle and plan, rechecks the complete
`ExecutablePlanRef`, and performs transaction-current validation through
`riffdb-commit`. No startup proof, `CommandPlan`, generic callback, or active
port is passed through storage to avoid that work.

#### Authoritative outcome/commit and event/outbox reciprocity

For every committed command, exactly one full stored outcome exists at its
idempotency identity and reciprocates with exactly one commit at the same
sequence. The outcome table key matches its payload identity; outcome and commit
agree on every shared immutable field; the linked provenance record repeats the
same identity; and no matching pending row remains. A missing, duplicate,
mismatched, orphaned, or still-pending outcome/commit/provenance edge is
`CorruptData`, fails authoritative readiness, and is never repaired or replaced
with a synthesized pointer.

The command record set freezes one reciprocal authoritative graph. For every
event ordinal named by a commit record, exactly one durable event and exactly one
outbox intent with the same `EventId` and equal canonical event identity/payload
exist. The event and intent each identify that same commit and ordinal, and the
commit identifies both. No authoritative event or intent may be orphaned,
duplicated, linked to different commits, or disagree about canonical content. An
authoritative commit, event, or intent missing any reciprocal member is
`CorruptData` and fails core authoritative readiness; a delivery-status row can
never be used to synthesize the missing member.

Outbox delivery status is a separate rebuildable worker-state overlay. A missing
status row means never-attempted semantic `Pending`; an explicit `Pending` row may
retain bounded retry metadata. Any present status row must name an existing
reciprocal authoritative event/intent/commit tuple; an orphan status is a derived
integrity finding and is never treated as an intent. On clean restart, WP-160
uses only its specialized typed transitions to normalize recoverable delivery
states, including converting an interrupted `Delivering` lease into the exact
retryable pending state defined by its worker policy. It does so only after
composed authoritative readiness has succeeded; this is an execution precondition,
not a new manifest dependency on WP-070. It may neither invent nor rewrite an
event, intent, commit, outcome, or provenance record. An undecodable or otherwise
non-normalizable status degrades the outbox subsystem and blocks delivery until
operator action or an accepted worker-state recovery path; it does not by itself
make an otherwise valid authoritative database corrupt.

Projection rows, apply markers, generations, and frontiers are likewise derived
from the authoritative commit log. WP-070 validates and reports their structural
and cross-link findings but does not advance, truncate, delete, or rebuild them.
After authoritative readiness succeeds, WP-170 owns typed projection recovery:
it verifies retained generation reciprocity and either retires/quarantines a
failed candidate and rebuilds only into a fresh generation under ADR-0010 and
ADR-0017, or marks that projection degraded when safe rebuild is unavailable. It
never lowers a published frontier or repairs or infers an authoritative commit,
entity, event, intent, outcome, or provenance record from projection state.

Readiness is therefore explicit rather than one undifferentiated flag:

- **Core authoritative readiness** covers metadata and sequence allocators,
  catalog/capability/audit records, pending and terminal idempotency state,
  entities and indexes, commit records, outcomes, provenance, durable events, and
  outbox intents, including all required reciprocity.
- **Outbox health** covers only delivery worker state after authoritative
  integrity succeeds.
- **Projection health** is reported per projection identity/generation and covers
  only rebuildable projection state after authoritative integrity succeeds.

Core reads and writes may remain ready when only a derived subsystem is degraded;
an operation that depends on that subsystem returns its typed degraded or
unavailable result. A shared-engine failure that prevents trustworthy table or
record isolation remains a core storage failure, not a derived exception. WP-130
exposes the composed core gate through P1 lifecycle and gRPC health. WP-185
extends that same graph and health view with P2 outbox/projection/observability
findings so a derived failure is never hidden as globally healthy; it does not
construct a second core graph. WP-070 owns structural read-only detection and
reporting, WP-160 owns outbox delivery normalization, and WP-170 owns projection
rebuild/degradation. None may claim another owner's validation or recovery
action.

#### Standalone service-audit append

ADR-0007 requires durable attempt audit before admitting protected mutations or
releasing protected administrative data. `riffdb-storage-api` therefore owns one
specialized `ServiceAuditAppendIntentV1`, one
`StoredServiceAuditRecordV1`, and one synchronous consuming append transition.
The service-owned input is bounded, pre-sequence, and contains no timestamp. The
commit coordinator obtains exactly one validated canonical value from its
synchronous `AdministrationClock`, then lowers the input into the storage-owned
`ServiceAuditAppendIntentV1`. That intent contains the checked safe audit fields,
phase, independent targets, exact closed result link, and trusted timestamp
defined by accepted ADR-0007. It contains no `AdministrationSequence`, raw
credential, digest, idempotency key,
entity/index/partition key, cursor, command input/output, free-form source or
error text, network address, transport object, callback, or arbitrary bytes.
`riffdb-service` retains ownership of its operation-level `ServiceAuditInput`;
WP-100 validates and lowers it into this storage append intent after obtaining
the administration-clock value. Timestamps need not be monotonic and never order
records; the assigned `AdministrationSequence` does. A clock failure occurs
before this storage transition and is the ADR-0007 audit outage, not a storage
fallback to request or authentication time.

Only the commit coordinator receives the append handle. The consuming transition
opens a short authoritative administration transaction, reads the shared next
administration sequence, assigns that nonzero value, constructs the stored
record, advances the metadata with checked arithmetic, writes exactly the one
record plus metadata, and commits. It accepts neither a caller-selected sequence
nor a caller-selected timestamp nor a generic record/key/value. It samples no
clock. It allocates no application `CommitSequence` and
cannot mutate catalog, capability, bootstrap, entity, command, outbox, or
projection state. Catalog, capability, bootstrap, and service-audit transitions
share the same `AdministrationSequence` allocator and are totally ordered by
their committed keys.

The closed result is durable appended record identity, proven abort, or the
existing closed storage error. `CommitStatusUnknown` fences further writes and
fails authoritative readiness; an adapter must not infer whether the record is
present or release the protected operation. Audit append never recursively
audits itself. The intent and stored payload allow at most 16 target references
and 64 KiB of semantic encoded content, dominated by the ADR-0006 envelope
ceiling and the tighter bounds of their constituent IDs. Overflow or a bound,
phase, target-tag, or canonical-order violation writes neither metadata nor an
audit record.

Bootstrap does not call this standalone append before its first authoritative
transition. Its dedicated storage operation accepts a bounded principal-less
`BootstrapServiceAuditStartV1` plus the capability-administration fields carrying
the same already validated `AdministrationClock` timestamp as required fields of
the checked bootstrap request. The start value contains the same safe operation,
request, ingress, target, and approval fields but fixes phase to `started` and
contains neither an administration sequence nor a result link. The caller cannot
guess the sequence that will be allocated. For a new bootstrap it first checks
every SPEC Section 13.3 emptiness predicate against transaction-current state,
allocates two consecutive `AdministrationSequence` values, and atomically writes
the service-audit
`started` record at the first value and the capability-administration record at
the second together with the marker, capability record, and digest lookup. The
marker and capability record link the second value; the started record links
that same authoritative transition with
`ServiceAuditLinkV1::ControlPlane { administration_sequence: second }`. This is
one closed bootstrap transition, not a generic audit operation with capability
authority.

For an exact bootstrap replay, that same typed operation validates the marker,
capability, normalized request, and digest cross-links, allocates one new
sequence, and appends a new principal-less `started` record using that
invocation's one validated `AdministrationClock` timestamp and linked to the
original capability-administration sequence. The storage transition derives that
closed `ControlPlane` link only after it validates the marker; no caller supplies
it. It writes no new capability transition and leaves every authoritative
capability field unchanged. A failed,
malformed, or mismatched candidate allocates no sequence and writes no durable
administration record. After either successful storage result, the service uses
the ordinary standalone append for the invocation's terminal `succeeded`
record before releasing success. `CommitStatusUnknown` during the compound
transition fences authoritative writes and returns no inference or successful
bootstrap result; recovery uses the retained credential through the same typed
operation.

### Process hard bounds

All input, runtime-result, collection, key, semantic-value, and component-size
bounds that are knowable before transaction-current state use checked arithmetic
and are rejected before unbounded allocation or transaction opening. Compiler-
or policy-specific limits may be lower. The v1 storage hard ceilings are:

| Boundary | Maximum |
|---|---:|
| Binding targets/observations in one command | 4,096 |
| Root-validation targets/observations in one command | 4,096 |
| Total source-binding, root-validation, and range targets in one command | 4,096 |
| Total read dependencies or validation targets in one command | 4,096 each |
| Entity mutations, index deltas, event intents, or outbox intents in one command | 4,096 each |
| Complete canonical entity field document or one event/outcome value | 1 MiB |
| Owned materialized snapshot encoded content | 16 MiB |
| One pre-commit intent or final commit-record semantic payload | 15 MiB |
| Commands staged in one write transaction | 64 |
| Aggregate encoded staged write set | 16 MiB |
| Entries returned by one generic storage scan page | 500 |
| Encoded content returned by one generic scan page | 4 MiB |
| Records returned by one internal ordered commit-scan page | 500 |
| Encoded content returned by one internal ordered commit-scan page | 16 MiB |
| Entity, index, partition, conflict key, or index prefix | 4 KiB |
| Integrity findings returned in one report | 256 plus a `truncated` flag |
| Catalog bundle semantic payload | 15 MiB |
| Service-audit target references / semantic payload | 16 / 64 KiB |
| Any encoded durable payload/envelope | ADR-0006 absolute 16 MiB ceiling |

The total intent/commit limits dominate the per-collection limits; satisfying a
count does not permit exceeding the byte budget. The exact aggregate staged-write
capacity is deliberately calculated inside the write transaction only after
influential current values, normalized old/new index entries, and affected epoch
positions are available, and it must be reserved before sequence assignment.
That bounded calculation uses a sequence-free semantic plan and conservative
WP-065-defined upper bounds for the complete canonical `StoredEnvelope` of each
record class. After assignment and exact graph construction, WP-070 recomputes
each actual envelope charge and rejects an excess before staging; unused reserved
headroom has no durable meaning. This is the only aggregate byte calculation
deferred past transaction open and does not permit an unbounded allocation.

The dedicated internal ordered
commit scan may therefore return any one valid commit record up to the absolute
16 MiB durable-envelope ceiling; it does not chunk a semantic commit. Public
service and transport APIs may impose lower response limits. Range reads that
would need a second generic page cannot influence one command in v1. Integrity
truncation reports the total as at least the returned count and never means the
unreported problems are accepted. Any authoritative finding, returned or
truncated, fails core readiness; a scan that establishes only derived findings
degrades the corresponding outbox or projection subsystem under the ownership
rules above.

### Closed storage errors and semantic results

`StorageErrorKind` is closed:

1. `Unavailable` -- the requested operation did not durably commit, or the
   backend proves it aborted; retry may be possible.
2. `CommitStatusUnknown` -- a commit attempt returned without proof of commit or
   rollback. The engine is fenced from further writes until reopen/integrity
   recovery, and command resolution uses idempotency.
3. `CorruptData` -- checksum, canonical encoding, cross-reference, metadata, or
   engine integrity failed.
4. `IncompatibleFormat` -- storage version, record type, schema hash, or required
   migration is unsupported.
5. `LimitExceeded` -- a checked semantic storage hard bound was exceeded before
   the operation could proceed.
6. `InvariantViolation` -- an impossible transition or internal state-machine
   condition was requested.
7. `SequenceExhausted` -- an application sequence, administration sequence, or
   index epoch cannot be advanced without overflow.

The public error contains only the kind and optional incident ID. Adapter source
errors and paths remain in trusted tracing keyed by incident ID; arbitrary engine
messages, keys, entity values, payload bytes, table names, and credentials never
cross the storage boundary. `Unavailable` may map to the approved public storage
failure. `CommitStatusUnknown` maps to outcome uncertainty for a submitted
command. Corruption or incompatibility in authoritative state, and authoritative
invariant violation or exhaustion, fail core readiness and map to an opaque
internal incident. A storage error confined to derived state degrades that
subsystem and cannot authorize an authoritative rewrite. Final mapping of a
caller-supplied limit violation is owned by the API-neutral service.

An attempted mutation-affected index-epoch advance from `Value(u64::MAX)` is a
backend-proven abort of the complete authoritative transaction. It occurs before
application-sequence assignment, leaves Pending byte-identical, and makes no
part of the command graph durable. Storage reports internal
`SequenceExhausted`; the current command executor result is the opaque
`InternalDefect`, the coordinator stops and becomes unready, and queued or
future submissions receive `CoordinatorStopped`. This path is not
`Unavailable`, `CommitStatusUnknown`, `OutcomeUnknown`, or
`CoordinatorFenced`; the latter remains reserved for uncertain write status.

Normal semantic states are not `StorageError`: missing entity, observed absence,
terminal replay, idempotency input mismatch, pending admission, dependency
change, catalog expected-version mismatch, already-revoked capability, projection
gap, and end of scan are closed typed results. No implementation parses error
strings to recover semantics.

### Multi-command staging and group durability

The POC may initially stage one validated command per transaction, but the
consuming protocol deliberately preserves bounded multi-command staging for the
SPEC group-durability mode. A coordinator may append up to 64 independently
admitted and validated commands while the aggregate write set remains within 16
MiB. Starting each appended candidate checks only the remaining command count.
That candidate is rechecked and validated against transaction-current state
including earlier staged writes, derives and reads affected epochs, then freezes
and reserves its exact sequence-free plan against the remaining semantic and
conservative encoded capacity. Only then does it receive the next contiguous
sequence and construct its complete atomic sub-record set.

A candidate resolved before staging returns its exact prior `EmptyBatch` or
`NonEmptyBatch` and is not partially added. The coordinator may commit an already
staged nonempty prefix and retry the invalid candidate in a later transaction; a
storage or integrity failure aborts the entire uncommitted batch. One durable
commit makes every staged command visible atomically, and the returned ordered
results correspond one-to-one with assigned sequences. Batching never
shares idempotency identity, merges command outcomes/events, reorders queue
admission, or permits storage to choose semantic grouping.

Group-flush scheduling, latency windows, fairness, and any future production-
default policy are deferred to WP-100/benchmark review; the POC has no implicit
default. Preserving staging in the interface does not enable group mode before
those policies and crash tests exist.

### Explicit coordinator durability selection

Coordinator construction has no implicit durability mode. Every production
constructor receives one explicit checked process-local value whose
closed values are `Sync` and `Group`; it does not implement `Default`, infer a
mode from the selected backend, or expose a zero-argument/fallback construction
path. `Memory` remains a semantic `DurabilityMode` value because memory-adapter,
codec, and model fixtures must represent it, but only test-only coordinator
construction may select it. The production constructor's input type cannot
represent `Memory`; having no argument is not a valid construction path.

The P1 `riffdbd` component graph explicitly selects and passes the code-level
`Sync` value. The production
server does not yet expose `Group`; the existing scheduling, fairness, latency,
crash-evidence, and human-review gate remains controlling. Thus `Sync` and
`Group` are the production coordinator value domain without making `Group` an
enabled POC server choice or choosing an MVP default. The exact selected mode is
passed through graph construction and recorded unchanged in the committed
outcome and commit record.

### Resolved grammar-v1 transaction defaults

This proposal resolves three SPEC Section 22.2 defaults so WP-080/WP-100 do not
freeze competing behavior:

1. A grammar-v1 command may read observations associated with multiple logical
   conflict domains only inside its one declared `PartitionKey`. Every domain it
   may mutate and every corresponding `ConflictKey` is derived, sorted, and
   acquired before evaluation. Every influential read outside those mutation
   domains is represented by canonical evidence and revalidated at commit. There
   is no dynamic capability acquisition, capability upgrade, or cross-partition
   mutation.
2. Write-influencing indexed range reads are not enabled by grammar v1. A future
   bounded indexed command-read IR must have separately accepted static target
   derivation and must represent any required exclusion with explicit conflict
   keys known before evaluation. `IndexRangeEpoch` is frozen now as a storage
   capability and backend-conformance boundary, not as permission for a compiler
   or runtime to expose a hidden indexed command read.
3. Grammar-v1 read-only commands are unjournaled. They create no pending or
   terminal command-idempotency record and receive no application
   `CommitSequence`. A required service audit is a separate administration-stream
   record and does not persist the read result. Generic request/protocol schemas
   may carry an optional request-correlation key, but it has no durable command
   idempotency or outcome-recovery promise for this class. A durable read-only
   outcome journal requires a future accepted ADR. ADR-0007 owns the reviewed
   additive public Execute status and wire-sentinel mapping for this result;
   those sentinels are rejected outside that status and never construct a zero
   semantic `CommitSequence`, provenance identity, or durability mode.

### Explicit deferrals

This decision does not define or enable:

- a generic CRUD/write/transaction API, SQL, joins, arbitrary transaction
  callbacks, distributed transactions, Raft, cross-partition mutation, or engine
  row-lock semantics;
- live or incremental snapshots in runtime, write-influencing unbounded scans,
  or indexed command reads without an accepted IR plan and epoch policy;
- durable idempotency or outcome journaling for read-only commands beyond the
  closed unjournaled grammar-v1 rule above;
- any terminal disposition of deterministic runtime arithmetic/resource faults
  other than accepted ADR-0012's exact transition;
- capability semantic records outside accepted ADR-0009, or capability
  Protobuf fields before its proto-owner interface PR;
- standalone service-audit semantics outside accepted ADR-0007, or its Protobuf
  fields before the focused proto-owner interface PR;
- projection group-key bytes, projection payload fields, lifecycle details, or
  wait-result fields not already accepted by ADR-0010; they require the separate
  reviewed projection-format/interface decision before durable use;
- outbox lease duration, backoff, connector policy, response/error retention, or
  dead-letter policy, which remain WP-160 concerns;
- online migration, online backup, repair, compaction policy, record garbage
  collection, idempotency retention, historical bundle collection, or capability
  digest migration;
- activation of group commit, Fjall as a production backend, replication, or a
  generic backend portability promise; and
- later semantic Protobuf field numbers. ADR-0006's single proto owner adds them
  only after their storage semantic DTO is reviewed.

## Options Considered

1. **Owned bounded snapshot plus coordinator-driven consuming transaction:**
   Selected. It keeps runtime deterministic and gives one explicit owner to
   validation, sequence, and atomicity.
2. **Live engine snapshot through runtime:** Rejected. It leaks lifetime and
   backend semantics and risks holding engine resources across evaluation.
3. **Engine-owned `commit(intent)`:** Rejected. It makes storage resolve plans,
   choose ordering, or trust stale predicate results, effectively turning the
   engine adapter into the coordinator.
4. **General transaction callback or raw write batch:** Rejected. It permits
   bypasses and cannot prove that callbacks avoid I/O, async suspension, or
   undeclared writes.
5. **Captured predicate values as proof:** Rejected. They do not re-evaluate the
   exact historical rule over transaction-current state.
6. **One monolithic storage trait:** Rejected. It gives unrelated workers and
   transports more authority than they need.
7. **Force single-command transactions forever:** Rejected. It would make the
   accepted group-durability mode require a breaking storage transaction redesign.
8. **Exact reviewed redb 4.1.0 baseline behind the semantic API:** Selected. An
   unreviewed version/features graph or engine-specific public surface is
   rejected; Fjall remains the isolated unchanged-contract comparison.

## Consequences

- WP-060 must expose owned values and a consuming synchronous state protocol,
  not the illustrative live GAT snapshot in SPEC 10.1.
- Snapshot materialization copies complete bounded records before runtime and may
  reject a request that exceeds count or byte budgets.
- The coordinator retains the exact immutable historical plan through validation;
  storage never interprets `CommandPlan` or assigns semantic outcomes.
- Semantic plan fidelity is represented by a coordinator-private proof and
  architecture boundary, while storage-owned DTOs prove only structural facts;
  no crate-cycle or public forgeable proof type is introduced.
- Memory/redb adapters implement meaningful atomic transitions rather than a thin
  untyped key/value facade.
- Sequence assignment is unreachable until the exact transaction-current
  mutation-affected epochs and sequence-free write plan have reserved semantic
  and conservative encoded capacity; every final canonical envelope is checked
  against that reservation before staging.
- Index-range validation uses lazily created exact leading-prefix epochs with an
  explicit before-first position, and standalone service audit shares the one
  ordered administration sequence through its dedicated append transition.
- Authoritative outcome/commit/provenance and commit/event/outbox-intent
  reciprocity are checked at readiness; missing delivery status means `Pending`,
  while delivery status and projection state remain typed rebuildable overlays
  with distinct health.
- New authoritative or derived transitions require storage-interface review,
  conformance cases, and a compatible durable schema before persistence.
- A commit whose backend result is uncertain cannot be reported as definitely
  failed; the engine is fenced and idempotency recovery determines the result.

## Compatibility

The exact plan-reference fields, observation/dependency variants and tags,
canonical order, absence/version meaning, validation-request/private-proof
boundary, intent boundary, influential-versus-mutation-affected epoch ownership,
count-only candidate start, sequence-free write plan, pre-sequence capacity
reservation, pre-stage canonical-envelope verification, sequence origins,
frontier and epoch-position
representations, prefix-bucket advancement, transaction transition loop, atomic
record membership, event/outbox-intent reciprocity, absent-status-as-`Pending`,
single-outcome terminal-row ownership, outcome/commit/provenance reciprocity,
core/subsystem readiness split and recovery ownership, bounds, error kinds,
specialized port semantics, standalone service-audit timestamp/link boundary,
clock-source rules, initialization-before-evidence ordering, exclusive session
and exact-end semantics, structural/catalog proof ownership and binding,
WP-130-only dormant-port activation, catalog-owned opaque command readiness/
resource evidence and current recheck, bounded lineage-overflow retention and
ordered reproduction, explicit process-local production coordinator durability
selection, and batch staging rules are semantic
compatibility boundaries.

Engine-private handles and physical access mechanics remain adapter
implementation details. The normative POC table/key layout and 26-type registry
are frozen by this accepted amendment; WP-065 freezes their durable messages and
fixtures before WP-070 implements them. Semantic records become durable only
through ADR-0006 proto-owner PRs.
Changing a durable field, key, table prefix, schema hash, or migration behavior
requires compatibility and recovery review.

## Security

Storage receives only authorization-resolved semantic values. It never resolves
tenant scope, capability grants, or caller claims. Raw idempotency keys, bearer
tokens, digest keys, untrusted provenance text, source snippets, entity values,
and raw logical keys are absent from safe errors, tracing fields, metrics, and
public debug output.

Redb's reviewed transitive unsafe code remains confined behind
`riffdb-storage-redb`; this decision grants no first-party unsafe exception. The
exact dependency review above and WP-070 crash/conformance evidence are both
required. A clean dependency scan does not substitute for durability testing.

Only the coordinator can obtain authoritative transaction progression. Runtime
has no ambient authority and transports have no storage port. Capability records
are cross-linked and accessed through their dedicated typed port; projection and
outbox workers cannot mutate authoritative entity, catalog, capability,
idempotency, outcome, or commit state.

New-database metadata initialization follows the same ownership rule. The
storage API owns the source-free probe and consuming atomic transition, while a
commit-owned `DatabaseInitializationExecutor` is the only production caller of
that transition. Server composition may generate and pass a checked candidate
through the executor after `NeedsInitialization`; it never receives the storage
mutation handle. Initialization completes before the exclusive structural
evidence session begins; neither the evidence session nor catalog validation can
initialize, repair, or otherwise mutate the database.

All lengths/counts are checked with overflow-safe arithmetic before allocation.
Malformed keys, records, plans, duplicate targets, unsupported schemas, and
unknown transition tags fail closed without returning partially validated state.

## Testing

One parameterized semantic suite runs unchanged over memory and redb, and over
the isolated Fjall experiment where its API can implement the same contract.
It includes:

- snapshot consistency, ownership/lifetime compile checks, complete-record copy,
  empty/present observations, range epochs, count/byte boundaries, and exact
  `BindingId` ordering;
- catalog evidence tests proving that one-byte-over valid expansion returns only
  the raw bounded snapshot and exact resolved plan/proof with no over-budget
  normalized record or mask; pure current-recheck tests prove exact raw equality
  and deterministic charge, while coordinator integration compares dependencies
  before invoking that recheck and terminalizing the reproduced overflow;
- exact whole-index and every leading-component epoch buckets, before-first,
  old/new prefix union and deduplication, covered-value changes, later commands
  seeing staged epochs, overflow rollback, and component-incomplete rejection;
- golden dependency tags/order and malformed/duplicate/trailing-byte rejection;
- mutation-between-snapshot-and-commit cases for every absence/version/epoch
  variant and every historical-plan/target/hash mismatch;
- proof that predicates are re-evaluated against transaction-current values and
  proposed post-images, not captured booleans;
- architecture checks that storage API may name contract IR only from its narrow
  projection-schema module and only for ADR-0017's two immutable checked schema
  value types, contract IR has no reverse storage dependency, storage API cannot
  depend on runtime/commit, `riffdb-proto` cannot depend on storage API, only
  storage API's narrow `proto_codec` may name Prost types, runtime cannot name an engine/transaction,
  only commit composition can receive a write handle, and storage exposes no
  callback, raw write, async method, semantic-proof marker, or transport type;
- startup type-state and architecture checks proving initialization precedes one
  exclusive `StructuralEvidenceSession`, no mutation or live service port is
  reachable during the session, every bounded namespace reaches exact end,
  catalog alone IR-validates session-scoped history, and only WP-130 can consume
  matching `StructurallyOpened` and `ValidatedCatalogHistory` values; storage and
  redb cannot name catalog, command IR, or the catalog proof;
- compile-fail/type-state tests for every invalid batch transition, including
  attempting to commit `EmptyBatch`, capacity before affected-epoch reads,
  sequence before capacity reservation, candidate reuse, staging an unequal or
  partial record set, and starting a second candidate from a candidate state;
- explicit tests that influential range dependencies and mutation-affected epoch
  targets may overlap, differ in either direction, and cannot be substituted;
- exact per-record-class conservative envelope-bound fixtures from WP-065 and
  memory/redb checks that equal-bound bytes stage, one-byte-over aborts before
  staging, unused headroom is harmless, and no failed reservation consumes a
  sequence;
- atomic state/model comparison for complete success, no-op outcome, replay,
  mismatch, dependency retry, and every staged-record failpoint;
- exact full-`StoredOutcomeV1` terminal-row fixtures proving pending deletion
  writes no tombstone or envelope, no second outcome/terminal record exists, and
  startup rejects every missing, duplicate, mismatched, orphaned, or
  still-pending outcome/commit/provenance edge;
- sequence/version tests starting at 1, zero-construction rejection, fallible
  decoding, contiguous allocation, rollback/restart, exhaustion, administration
  isolation, empty frontier/epoch positions, and no `EventId` at zero;
- single-command and bounded multi-command staging, staged-prefix behavior,
  aggregate bounds, one flush, ordered results, and whole-batch crash outcomes;
- construction/source-shape tests proving production coordinator creation
  requires an explicit `Sync` or `Group`, has no `Default` or backend fallback,
  and its input cannot represent production `Memory`, plus P1 composition tests
  proving `riffdbd` explicitly supplies code-level `Sync`, exposes no operator
  selector, and does not enable `Group`;
- error classification tests distinguishing proven abort from unknown commit,
  fencing after uncertainty, secret canaries, and readiness refusal;
- event/outbox model tests for every missing, duplicate, mismatched, and orphaned
  commit/event/intent edge; absent delivery status as semantic `Pending`; rejection
  of orphan status as intent evidence; and atomic rollback at every event/intent
  failpoint;
- recovery ownership tests proving WP-070 only reports authoritative versus
  derived findings, WP-160 alone normalizes interrupted delivery state after core
  readiness, WP-170 alone rebuilds or degrades projection state, and neither
  worker can mutate an authoritative record;
- catalog expected-version old-or-new recovery, capability cross-link and atomic
  audit transitions, compound bootstrap started/authoritative ordering and
  one-timestamp linkage, replay linkage, standalone audit input-without-timestamp,
  exact result links, commit-owned clock lowering, append bounds/canonical tags,
  shared administration ordering/fail-closed uncertainty, command/outbox
  atomicity, and projection state/frontier atomicity; and
- process kill/reopen and idempotent recovery at every SPEC storage failpoint,
  plus deterministic fixture regeneration for every durable DTO.

No correctness test uses sleeps. Memory tests use explicit barriers/hooks;
process tests use named failpoints and durable reopen.

## Requirements and Work Packages

- **Requirements:** `SYS-004`, `STO-001`, `STO-002`, `STO-010` through
  `STO-012`, `STO-020` through `STO-022`, `LOG-001`, `ENT-001` through
  `ENT-003`, `TXN-002`, `TXN-030`, `TXN-031`, `TXN-040` through `TXN-044`,
  `EFF-001`, `EFF-003`, `REC-001` through `REC-003`, `PRJ-001` through
  `PRJ-004`, `MCP-046`
- **Defines or blocks:** `WP-050`, `WP-060`, `WP-070`, `WP-075`, `WP-080`,
  `WP-100`, `WP-110`, `WP-120`, `WP-130`, `WP-160`, `WP-170`, `WP-180`,
  `WP-185`, and the focused durable proto-owner interface package
- **Final evidence:** `WP-190`, `WP-200`

### Required companion and work-package reconciliation

The pre-acceptance `work_packages.yaml` could not authorize all of this decision
as written. The accepted reviewed governance batch makes the exact metadata
changes below; affected implementation must follow the reconciled manifest:

1. Advance WP-010's completion revision through a focused foundational interface
   follow-up under its existing `Cargo.lock`, `crates/riffdb-types/**`, and
   canonical-fixture paths. Add ADR-0004 to that follow-up's consulted/required
   decisions and add the nonzero assigned sequence/version types,
   `FrontierPosition`, fallible `EventId` decode, and zero-rejection tests to its
   deliverables and exit evidence. Every downstream upstream revision must name
   that advanced WP-010 revision.
2. Add ADR-0007, ADR-0012, and ADR-0017 to WP-060 `required_adrs`; add `MCP-046`
   to its requirements; and add the standalone audit DTO/append transition, the
   compound bootstrap transition request/result, authoritative event/intent
   reciprocity, absent delivery status as `Pending`, split integrity findings,
   epoch-position semantics, and private-proof/architecture conformance cases to
   its deliverables. Add
   `Cargo.lock` to WP-060 because wiring its root-workspace
   crate dependencies updates the generated lockfile. No WP-060 path authorizes
   edits to foundational types or Protobuf sources.
3. Update SPEC Sections 4.1, 5.2, 9.1, 9.5, and 17.3 and WP-080's
   objective/deliverables so deterministic runtime returns `EvaluatedCommand`,
   while the coordinator combines it with the exact stored admission into the
   final self-contained `CommitIntent`. Add ADR-0007 to WP-080 `required_adrs`
   because that split owns the admitted-provenance exclusion. The deterministic
   history property compares `EvaluatedCommand`; coordinator tests compare the
   assembled intent. Add `Cargo.lock` to WP-080 for its root-workspace dependency
   wiring; its existing source/test paths and acceptance commands remain
   sufficient.
4. Update SPEC Section 22.2 with the three closed grammar-v1 transaction
   defaults above. Add ADR-0004 to WP-040 `required_adrs` and rejection fixtures
   for dynamic/cross-partition mutation acquisition and write-influencing indexed
   range reads. Add the one-partition cross-domain evidence cases to WP-080 and
   add unjournaled read-only command cases to WP-100, proving no pending/terminal
   idempotency record or application sequence while any required service audit
   remains in the separate administration stream.
5. Add a focused `WP-065` durable semantic-record schema package depending on
   WP-020 and WP-060. It requires ADR-0004, ADR-0005, ADR-0006, ADR-0007,
   ADR-0009, ADR-0010, ADR-0011, ADR-0012, ADR-0013, ADR-0014, ADR-0016, and
   ADR-0017 and may edit only `Cargo.lock`, `proto/**`,
   `crates/riffdb-proto/**`, `fixtures/proto/**`, `scripts/generate-proto*`,
   `crates/riffdb-storage-api/Cargo.toml`,
   `crates/riffdb-storage-api/src/lib.rs`,
   and `crates/riffdb-storage-api/src/proto_codec/**`. Its deliverables are the
   reviewed `riffdb.storage.v1` semantic-record messages, descriptors, schema
   hashes, goldens, historical registrations, wire validation, and checked
   semantic mappings, including both records in the compound bootstrap
   transition and their linkage. `riffdb-proto` must not acquire a storage-API
   dependency.
   Its acceptance commands are `cargo test -p riffdb-proto -p
   riffdb-storage-api` and `./scripts/generate-proto --check` with a clean
   generated diff.
6. Add WP-065 as a hard WP-070 dependency and as an explicit P1 gate member
   without removing any existing gate member. Add ADR-0007, ADR-0012, and ADR-0017
   to WP-070 `required_adrs`, `MCP-046` to its requirements, `Cargo.lock` and
   `tests/service_audit_recovery/**` to its allowed paths, and durable standalone
   audit append, compound-bootstrap/replay crash and reopen evidence, complete
   authoritative reciprocity checks, and separately classified derived-state
   findings to its deliverables and recovery matrix. WP-070 reports structural
   findings and yields the exclusive evidence/session type state; full core
   authoritative readiness requires the matching WP-050 catalog proof and
   WP-130 lifecycle gate, including allocator progress and a valid active
   catalog. WP-070 never normalizes outbox status or rebuilds/degrades a
   projection. `crates/riffdb-storage-redb/Cargo.toml`,
   already inside WP-070's
   allowed crate path, owns an external `[[test]]` named
   `service_audit_recovery` whose path is
   `../../tests/service_audit_recovery/service_audit_recovery.rs`; its command is
   `cargo test -p riffdb-storage-redb --test service_audit_recovery`. WP-070 may
   not define a record schema while implementing redb. After adding exact
   `redb` 4.1.0 to the root lockfile, WP-070 must also run the repository-root
   `cargo deny check`; the isolated review above cannot replace that command.
7. Add ADR-0007's separate formal WP-127 public-schema package with direct hard
   dependencies only on WP-020 and WP-120, add it as an explicit P1 member, and
   add it as a WP-130 dependency. It owns public messages, descriptors, schema
   hashes, goldens, and wire-structural checks; WP-130 owns service-to-Protobuf
   semantic conversions. WP-127 does not own durable schemas, does not directly
   depend on WP-065, and does not reopen WP-020.
8. WP-100 already requires ADR-0007 and ADR-0012 and may implement the executor
   under `crates/riffdb-commit/**`. Add `MCP-046`, the bounded
   `AdministrationAuditExecutor`, commit-owned synchronous `AdministrationClock`,
   exact clock-source/sample behavior, shared administration-sequence behavior,
   and started/terminal invocation records including pending resume, terminal
   replay, and bootstrap's compound started/authoritative plus post-commit
   succeeded behavior to its deliverables; add `Cargo.lock` and
   `tests/service_audit/**` to its allowed paths.
   `crates/riffdb-commit/Cargo.toml`, already inside WP-100's
   allowed crate path, owns an external `[[test]]` named
   `service_audit_ordering` whose path is
   `../../tests/service_audit/service_audit_ordering.rs`; its targeted
   ordering/fail-closed command is
   `cargo test -p riffdb-commit --test service_audit_ordering`. It consumes the
   storage append and never exposes that handle to `riffdb-service`.
9. Add `Cargo.lock` to WP-050 for its root-workspace catalog/storage/IR dependency
   wiring. This generated-file authority, like the WP-060/WP-080/WP-100 additions,
   does not approve a new third-party dependency or feature set; those retain
   their ordinary dependency and security review.
10. Keep recovery action with the worker that owns the derived state. WP-160
    runs only after composed authoritative readiness succeeds and uses typed
    outbox transitions to normalize recoverable delivery status, including
    interrupted `Delivering`; an absent row already means `Pending`. WP-170 uses
    the same execution precondition and either rebuilds into a fresh projection
    generation from the commit log or exposes its typed degraded state. Neither
    package repairs authoritative data, and WP-070 does not perform either action
    during open/readiness validation. This adds no undeclared hard dependency.
11. WP-050 owns exhaustive historical IR/catalog validation over the borrowed
    session reader and returns the opaque `ValidatedCatalogHistory`; WP-070 owns
    the exclusive structural session and `StructurallyOpened` output. WP-130
    owns their matching, the production core composition, and the runnable P1
    readiness gate. This split introduces no redb-to-catalog/IR edge and passes
    neither catalog proof nor `CommandPlan` through a storage trait. WP-185
    reuses and extends the activated graph for P2 workers, MCP, and observability.
12. The 2026-07-14 clarification adds no work package, dependency, allowed path,
    or acceptance command. WP-060 freezes the complete candidate type-state and
    memory conformance; WP-065 owns canonical envelope codecs, per-record-class
    conservative upper-bound proofs, and boundary fixtures; WP-070 enforces the
    ordering and actual-envelope checks in redb; WP-100 supplies coordinator
    semantic/failpoint evidence; and WP-190/WP-200 retain the final crash and
    end-to-end evidence. No package may assign a sequence first and treat a later
    size failure as an acceptable gap.
13. The 2026-07-21 clarification requires manifest reconciliation but no new
    package, hard WP dependency, allowed path, or acceptance command. WP-050 adds
    `TXN-042` and ADR-0023, and owns the opaque `Ready`/resource evidence, pure
    current-recheck API, and one-byte-over returned-evidence fixtures under its
    existing catalog paths. WP-100 consumes
    that API and owns dependency ordering and terminalization without catalog
    path authority. WP-100 also owns explicit process-local production
    durability construction. WP-130 explicitly passes code-level `Sync` in its
    component graph, exposes no POC operator durability selector, and keeps
    `Group` server-disabled. Existing WP-050, WP-100, and WP-130 test commands
    provide the evidence.

ADR-0007 was accepted in the same governance change and cross-references this
specialized lower transition and proto-owner sequence. ADR-0005's complete-plan-
reference amendment is part of that same atomic change. These reconciliations do
not permit implementation to alter the accepted storage, protocol, or durable
semantics.

## Accepted Decisions

Acceptance of this exact record decided:

1. the five-field `ExecutablePlanRef`, including `ContractBundleHash`, together
   with the mandatory ADR-0005 cross-reference amendment;
2. the storage-API-owned, runtime-produced `EvaluatedCommand` result plus
   coordinator assembly of the final self-contained `CommitIntent` from the
   immutable stored admission;
3. storage-structural public DTOs plus a coordinator-private semantic proof and
   architecture-enforced write authority instead of a cyclic or forgeable
   cross-crate proof;
4. nonzero application/administration sequences, entity versions, and assigned
   index epochs, with explicit before-first frontier and epoch positions and the
   exact foundational constructor changes above;
5. exact leading-component index-epoch buckets, lazy before-first state,
   old/new affected-prefix union, once-per-command advancement, and overflow
   failure;
6. the v1 process bounds and closed storage errors, including the 64 KiB/16-target
   standalone service-audit bound;
7. the stable-Rust `EmptyBatch`/`NonEmptyBatch` candidate loop, maximum 64
   commands/16 MiB, exact-prior-batch nonfatal result, staged-prefix option, and
   whole-batch fatal abort;
8. the closed grammar-v1 defaults: one partition for all cross-domain reads,
   upfront acquisition for every mutation domain, complete influential evidence,
   no enabled write-influencing indexed range reads, and unjournaled read-only
   commands with no durable idempotency or application sequence;
9. the ADR-0007-compatible standalone audit DTO/append, service input without a
   timestamp, commit-owned `AdministrationClock`, exact clock source/sample rules,
   coordinator-only shared administration-sequence allocation, and proto-before-
   redb ordering;
10. authoritative commit/event/outbox-intent reciprocity, absent status as
    semantic `Pending`, separate core/outbox/projection health, WP-070 report-only
    recovery validation, WP-160 delivery normalization, and WP-170 projection
    fresh-generation rebuild/degradation without lowering a published frontier;
    and
11. direct `redb` 4.1.0 use only in `riffdb-storage-redb`, with default features
    disabled, no optional features, the reviewed dependency/unsafe/build/license
    surface and isolated policy-eligibility evidence above, mandatory WP-070
    root-lock `cargo deny check` and durability evidence, and new review for any
    dependency-graph change; and
12. source-free initialization before an exclusive structural evidence session,
    bounded exact-end structural and catalog-history validation, catalog-owned
    process-local `ValidatedCatalogHistory`, storage-API-owned and redb-produced
    `StructurallyOpened` dormant ports, and WP-130-only matching/activation
    without a storage-to-IR
    edge or any weakening of per-command historical-plan revalidation.
13. bounded IR-opaque evidence for every persisted entity, index-entry, and
    range-prefix key, with catalog-owned exact historical `KeySchema` validation,
    plus a dedicated 500-record/16 MiB internal ordered commit-scan page ceiling
    that leaves the generic scan ceiling at 500 entries/4 MiB.
14. count-only candidate start followed by admission/current-dependency recheck,
    private validation and mutation-affected prefix derivation, same-transaction
    affected-epoch reads, an exact sequence-free `CommandWriteSetPlanV1`, semantic
    and conservative encoded-capacity reservation, and only then sequence
    assignment, exact graph construction, retained-candidate verification,
    canonical per-class envelope-bound proof, and staging.
15. the frozen physical key rules, exact metadata keys, lineage/version bundle
    identity, one-row active catalog, 26-type registry, and authoritative
    standalone event row with atomic three-way event reciprocity; and
16. direct `sha2` 0.11.0 use with default features disabled solely for backup
    manifests, plus a dependency-free WP-070 storage benchmark harness and no
    Criterion dependency; and
17. one full `StoredOutcomeV1` envelope as both terminal idempotency state and
    persisted outcome, atomic pending-row deletion without a tombstone or second
    terminal envelope, and mandatory startup outcome/commit/provenance
    reciprocity validation without repair;
18. catalog-owned opaque `Ready`/resource evidence and pure current recheck:
    valid over-limit returned evidence retains only the original bounded raw
    snapshot and exact resolved plan/proof with no over-budget normalized data or
    masks, followed by coordinator-owned dependency-first invocation and
    deterministic overflow reproduction before `ResourceLimit` terminalization;
    and
19. explicit production coordinator durability construction over only `Sync` or
    `Group`, test-only `Memory`, no implicit default or backend inference, and
    explicit `Sync` selection by P1 `riffdbd` while `Group` remains disabled.

These semantic details received explicit maintainer review on 2026-07-13; they
were not inferred merely from the earlier direction approval. Item 14 and its
associated bounds/testing reconciliation and items 15-17 received explicit
maintainer review on 2026-07-14. Items 18-19 received explicit maintainer review
on 2026-07-21.

WP-060 may start after its declared package dependencies merge, but cannot be
completed until it implements the following accepted interfaces together:

1. accepted ADR-0007's service operation/audit record and emission semantics are
   implemented by the specialized append;
2. accepted ADR-0012's fixed transaction context and terminal execution-fault
   admission are reflected in pending and terminal DTOs;
3. accepted ADR-0009's capability record and ports are implemented;
4. accepted ADR-0017's projection group keys, payloads, generations, and
   frontiers are reflected before projection DTOs or rows freeze; and
5. the work-package and foundational/proto-owner reconciliation above is merged
   before its interface freeze.

No conflict was found with authoritative text in starting assigned application
or administration sequences at 1. SPEC requires unsigned contiguous sequences
but does not define the first assigned value. SPEC 15.2 uses a `CommitSequence`
for an illustrative frontier without defining the empty state; the explicit
`BeforeFirst` variant is therefore an accepted clarification, not a silent
reinterpretation. A future choice of zero as an assigned sequence, a different
empty-state representation, or eager epoch-prefix creation requires a
superseding accepted ADR before incompatible fixtures merge.

## 2026-07-22 V2 migration and aggregate-cap amendment

ADR-0039 is authoritative for the following narrow additions and supersedes
older statements in this record only where they conflict with these additions:

- `HistoricalSemanticEvidence::IndexMigrationRow` replaces
  `PersistedKey(IrOpaquePersistedKeyV1::IndexEntry)` for each physical V1 or V2
  index row. It retains the former index-entry `0x04` order key, precedes all
  `0x05` capability-partition evidence, and binds the physical key, checked V1
  or V2 semantics, and exact canonical envelope through the WP-065 codec. The
  initial scan records only whether V1 was observed and retains no row or
  instruction; only the bounded migration rescan produces one fresh row and
  exactly one closed migration instruction.
- `riffdb-catalog` may use `riffdb-invariant` only for pure, bounded evaluation
  of a selected historical aggregate `partition_expression`. This adds no
  storage, runtime, commit-check, I/O, time, entropy, callback, or reverse
  dependency.
- Startup is linear and session-bound. Catalog returns only `Ready` or
  `MigrationRequired`; storage returns only `Clean` or `MigrationRequired`.
  Only matching pairs may be joined. Migration uses one-bundle states, no
  storage transaction spans catalog work, and compare-and-rewrite batches are
  atomic. Completion returns a dormant unopened backend; readiness requires a
  complete new startup session with a fresh `OpenSessionId`. There is no
  migration marker or operational migration port.
- Migration evidence and instruction/write-batch ledgers are independently
  capped at 500 rows and 4 MiB per page, while the existing general historical
  and bundle bounds remain unchanged. WP-065 must prove that one maximum valid
  row and its conservative replacement fit.
- `riffdb-storage-api` owns the fields-private result
  `Fits(EncodedWriteSetUpperBound) | ExceedsAcceptedAggregateCap`, produced only
  by the WP-065 codec after every record charge and checked aggregate sum have
  succeeded. WP-100 alone maps the exact aggregate-cap branch to its private
  `CapacityUnavailable` decision and existing public `StorageUnavailable`,
  before sequence assignment and without a write or readiness loss. Per-record,
  overflow, encoding, compatibility, reservation, and integrity failures remain
  fatal and are never mapped through that branch.

Corrective ownership is WP-060 for storage type-state and memory behavior,
WP-065 for the source, codec, registries, evidence/result factories, and bound
proofs, WP-050 for catalog derivation, WP-070 for redb migration, and WP-100 only
for consuming the aggregate-cap result. ADR-0039 contains the complete package
and fixture reconciliation.

## Decision Deadline

Exact acceptance is required before WP-060 merges a public snapshot,
dependency, validation request, intent, durable DTO, persistence port, or
transaction trait. The ADR-0005 amendment must accompany acceptance. The WP-010
follow-up and manifest reconciliation must precede WP-060 implementation;
WP-065 proto-owner records and exact redb dependency approval must precede
WP-070 physical persistence. None may choose a competing shape first.

## Amendment 5 — redb baseline advanced to 4.2.0 (Accepted 2026-08-18)

The maintainer accepted this exact text on 2026-08-18. The approved
embedded-storage baseline becomes exactly `redb` 4.2.0 with default features
disabled and no optional features, directly owned only by
`riffdb-storage-redb` (and, for the deterministic simulator's backend seam,
`riffdb-sim`). Every obligation in the Decision section above stands: redb
remains private behind the semantic storage API, no other crate may depend on
it directly, and any dependency-graph or feature change still requires renewed
human review.

- **Why the pin advances.** The 4.1.0 pin carries a durability defect this
  repository's own seeded campaign found and reported upstream: its one-phase
  commit orders `set_len` growth against the in-commit header write only
  through the commit's final fsync, so a torn crash inside a file-growing
  commit can durably keep the header while losing the extension, and 4.1.0
  then fails at every subsequent open instead of repairing. Upstream
  `fd82ced` fixes it, and 4.2.0 is the first released version to contain the
  fix. Staying on a pin with a known unopenable-database state is the larger
  durability risk.
- **Fix verified in the vendored source, not assumed.**
  `PageManager::grow` calls `Storage::sync_file` so a file extension is
  durable before the larger layout can reach the on-disk header, and a file
  actually truncated below its stored layout now returns
  `StorageError::Corrupted` from the header check rather than tripping an
  open-time assert. `REDB_PIN_CONTAINS_FD82CED` flips to `true` with the pin,
  and the manifest guard that reds when the pin moves keeps the two in step.
- **Observed behaviour change inside the fault simulator.** Moving the growth
  sync ahead of the header write moves the physical operation stream, so
  seeded crash placement moves with it. The interrupted-commit-PRESENT
  territory becomes rarer rather than unreachable: 3 of 90 completed campaigns
  under the 4.1.0 pin, 1 of 234 under 4.2.0. Four campaign and corpus
  witnesses were rotated under the corpus's documented procedure, each rerun
  12/12 on every expectation before pinning, and three historical entries that
  reach their original territory again carry a `restored_by` receipt so the
  corpus records the whole history.
- **One precondition no longer holds.** A quiet-schedule `Immediate` commit
  now leaves exactly one mutation written but unsynced at the crash point used
  by `simulated_store_reopens_through_dirty_shutdown_repair_after_a_crash`,
  because `sync_file` makes the file durable without flushing the in-memory
  write buffer. Durability is unaffected: recovery keeps the write, both
  acknowledged rows are present after reopen, the torn-resolution sweep over
  real keep/drop/truncate decisions still proves the acknowledged rows survive
  exactly, and the 86-arm recovery matrix is green. Whether to re-establish an
  all-synced crash point or restate that test's claim is a separate decision
  and is not settled by this amendment.
- **What this amendment does not grant.** It does not make the pin
  release-eligible. The renewed dependency-graph, feature, and unsafe-surface
  review this record requires for any redb change is still outstanding, as is
  the portable two-profile re-baseline, which must run against the settled
  PostgreSQL comparator rather than the unsettled one whose repetitions
  differed by 1.9x within a single run.
