# WP-824 implementation progress

## Admitted provider policy execution

Provider policy observations now read from the same captured immutable root
through admitted entity decoders and bounded forward relationship range pages.
Every point/page call checks the existing worker pin for cancellation. Tokenized
admission uses its captured source rather than reopening the live database.
The shared authorization kernel, exact candidate coverage, partition checks,
ordinary scan ceilings and current final authorization remain unchanged.

Before row copies, probes, evaluation nodes, argument vectors and row-hash
encoding, borrowed native ownership and compiler-owned read operands determine
construction reservations. Observation keys and proof vectors are admitted
before allocation. Successful proofs settle their native capacity after row
observations drop; aliases retain the charge until final destruction. Delta
post-image policy evaluation keeps its normalized post-images and admits their
workspace before copying or evaluation.

Tests compare admitted range pages with ordinary pages, including continuation,
immutable source isolation, physical-key substitution and final-alias release.
Policy observation tests prove pressure refusal before reads, missing-row denial,
large-row construction and release of row workspace before retaining the proof.
The first combined run passed 693 of 694 tests; the delta test exposed a missing
proof-settlement census reservation, which was corrected before rerunning.

D003 decision: use a hidden first-party admitted policy reader and the existing
policy kernel; its relationship primitive accepts only forward requests, the
sole direction used by policy probes. Unsupported requests fail closed before
allocation. No application-facing interface, durable format, ordering or
authorization guarantee changes.

PR note: Package WP-824; Tier guarantee; Behavior: admitted provider policy
source/evaluation/proof ownership; Compatibility: unchanged policy semantics,
source checks, page ceilings and durable bytes; updated handbook configuration
page; Hazards/follow-ups: qualified plan retirement, full CI and qualification
remain open. Checks: all 695 selected server/policy/query-executor tests pass,
including the process crash oracle; the focused admitted storage range test
passes; all seven scoped static checks pass. Full CI has not yet run.

## Admitted source receipt and pin validation

Provider replay now admits native shallow receipt vectors, mutation-fold trees,
key/value buffers, encoding/hash work and census scratch before decoding.
Successful immutable receipts settle their final native capacity charge; aliases
retain it through final destruction. Failed admission leaves the cursor position
unchanged and fused. Provider-opened cursors retain their bounded source-vector
metadata charge and reject unguarded advancement or a foreign accounting graph.

Opening source pins, checking resume/interior positions, validating terminal
roots and checking a retained fence observation all use admitted receipt work.
Ordinary readers keep their original behavior. Archived stubs resolve the same
retained frame hash and physical custody checks: resolver/path inventories and
physical buffers are admitted before I/O, then borrowed frame counts admit native
frame and receipt-fold construction before decoding. Proven retained frames skip
command-segment count re-derivation exactly as the existing resolver does; their
mutation values remain opaque. Journal decoding also rejects impossible mutation
counts before allocating the declared vector. No durable bytes change.

Provider head observation now admits a small snapshot descriptor and reads the
existing immutable frontier directly, which is the same metadata used for a
commit scan's inclusive upper fence. Captured workers check that frontier before
pin validation. Delta history-point hashing separately reserves its encoding
buffer after an admitted receipt has settled.

Tests cover saturation without progress, fused refusal, native/final-alias
accounting, large payloads, exact end under pressure, foreign pools, archived
byte equality, oversized placements, hostile counts and checked overflow. The
initial 1,693-test selection passed 1,688 tests. Four server tests failed and one
timed out because an attempted above-head scan violates exact-end page framing.
The implementation now reads the immutable frontier directly; all 551 selected
server tests passed on rerun, including the daemon crash oracle. The storage API
and redb selections had already passed. All seven scoped static checks passed.

PR note: Package WP-824; Tier guarantee; Behavior: admitted source receipt and
pin validation, retained cursor/receipt ownership and bounded frontier observation;
Compatibility: unchanged receipts, hash/framing/custody checks, source ordering,
query/authorization meanings and application interfaces; Handbook: Configuration;
Hazards/follow-ups: policy execution admission, qualified plan retirement, full CI
and measured lifecycle qualification remain open.

## Exact catalog capture from ordinary metadata publication

Primary providers now probe the catalog adapter's existing ordinary-service
publication. Exact contract preparation warms that publication on its ordinary
blocking path; inline exact contract and named-query hits require current active
metadata, with misses returning to ordinary preparation. Warming remains
best-effort for ordinary historical reads, preserving their existing behavior.
Provider capture checks the immutable source's exact durable pointer and admits
the complete borrowed shared graph before cloning. It cannot cold-read or decode
a catalog. Missing/stale/contended publications and budget pressure refuse typed.

Follower providers similarly borrow the already validated completed-prefix
catalog publication after matching the captured source pointer. No provider
metadata worker or uncharged cold fallback is introduced. Unit storage fixtures
prepare their ordinary catalog publication before constructing an exact runtime.
The retained physical source descriptor now carries its own conservative metadata
charge through selected-source clones and successor publication. Shared catalog
charges follow actual final data references, including ordinary publishers and
retained stored lineage names.

Focused tests cover cold/stale refusal without bundle decoding, exact publication
identity, lock contention, saturation before cloning, duplicate capture and final
shared-reference destruction. An initial probe admitted no parent allowance for
charge splitting; the tests caught it and the probe now retains its bookkeeping
allowance. Final-release assertions also explicitly drop the stored fixture
repository's shared lineage reference. All 551 selected server tests passed,
including the real-daemon crash oracle; 72 tests were skipped or excluded. All six scoped
static checks passed. Receipt and policy execution admission remain open.

PR note: Package WP-824; Tier guarantee; Behavior: admitted exact catalog capture
from existing ordinary/follower publication and retained source metadata;
Compatibility: unchanged catalog bytes, validation, historical query meanings,
authorization and application interfaces; Handbook: Configuration;
Hazards/follow-ups: receipt/policy ownership, qualified supersession and full CI.

## Private source row conversion workspace

Full text, predicate, nullable-predicate and tokenized rebuilds now reserve native
conversion forests before copying source values, keys, output records or
reference-cell maps. Borrowed predicate/order traversal admits profile metadata
before map construction. Tokenized candidate populations use the same admitted
immutable-key builder. Private row guards destroy their rows before releasing
workspace; text conversion retains that workspace through index preparation.
Delta changes retain reservations through mutation-vector conversion, including
deletes, key decoding, projected output and policy observation copies.

Tests cover pressure refusal before growth, a native census of large-value and
reference-cell conversion forests, and data-before-reservation destruction.
All 549 selected server tests passed, including the real-daemon checkpoint crash
oracle; 72 tests were skipped or excluded. All six scoped static checks passed.
Complete catalog capture, receipt cursor and policy execution workspace remain
open; this change does not establish qualification readiness.

PR note: Package WP-824; Tier guarantee; Behavior: admitted private source rows
and delta conversion; Compatibility: unchanged bounds, checks, row meanings,
formats and application interfaces; Handbook: Configuration; Hazards/follow-ups:
remaining catalog/receipt/policy ownership and full CI.

## Derived record materialization admission

Worker materialization now accepts an already admitted entity record from the
same accounting graph. It reserves census scratch before inspecting borrowed
canonical value capacities, then reserves native field-vector growth, existing
nested-value copies, missing-position/null masks, encoding buffers and fixed
metadata before cloning or normalizing. Exact writer, lineage, key, schema and
null-fill validation still runs through the existing materializer.

An unchanged result shares the complete original admitted record. Null insertion
creates fresh private fields and encoding; their actual native capacity is sealed
in an original owner before a semantic clone can escape. Those clones retain the
original charge through final field-data destruction. The worker separately
admits the temporary executing-binding clone. Delta post-images use the existing
admitted entity decoder and retain admitted materialized records while private
changes survive. No newer point read substitutes for a receipt post-image.

Focused tests cover existing null-fill behavior, unchanged owner identity,
foreign/saturated accounting refusal, final semantic-field captures, 256-KiB
values, arithmetic overflow and invalid exact bindings.
The broader 1,175-test selection passed 1,174 tests; its one failure was a catalog
fixture hard-coding the old 128-byte accounting root. The fixture now uses the
reported fixed charge, and all 127 catalog tests passed on rerun. All seven scoped
static checks passed. Remaining source row
collections, receipt cursor work and full catalog/policy assembly still require
admission composition. WP-824 remains open.

PR note: Package WP-824; Tier guarantee; Behavior: admitted derived-record
materialization and delta entity decoding; Compatibility: unchanged schema/key/
writer checks, bytes, ordering and application interfaces; Handbook: Configuration;
Hazards/follow-ups: remaining collection/catalog/receipt/policy ownership, full CI.

## Source candidate population ownership

Text, predicate and nullable-predicate full builds now reserve private candidate
populations before constructing their trees. Each distinct key reserves its
buffer and a conservative full B-tree node before cloning or insertion. Repeated
keys admit no new allocation. Deletions keep conservative private work until
settlement; failure destroys keys before releasing construction capacity.

Selected source proofs retain one admitted immutable population. Clones share
its final-reference charge. Delta preparation copies keys through the same
admission path, preserving private successor isolation and the existing
delete-before-insert population and capacity semantics. Final population charges
drop only after their last data reference. The existing 100,000-candidate ceiling
is unchanged.

Tests cover pressure refusal before mutation/copy, duplicate stability, independent
successor replacement, final capture retention and a native capacity census at
the complete existing population ceiling. All 547 selected server tests passed,
including the real-daemon crash oracle; 72 tests were outside the selection or
already ignored. All six scoped static checks passed. Source rows, receipts, materialization
workspace and catalog/policy assembly still require complete composition; this
change alone does not establish qualification readiness.

PR note: Package WP-824; Tier guarantee; Behavior: admitted source candidate
history; Compatibility: unchanged source proofs, row semantics, formats and
application interfaces; Handbook: no application/operational behavior changes;
Hazards/follow-ups: finish remaining source/catalog/policy ownership and full CI.

## Projection section admission

Borrowed projection framing now computes checked native bounds before complete
plan parsing. Admission covers plan/vector/name/type/constant ownership, record
name sets, arena construction and reachability, declared enum-registry copies,
hash dependency forests and the existing format-bounded hash writer. Unknown
result-type enum references are bounded independently of the declared registry;
invalid input cannot make hash traversal exceed a valid-only estimate.
Canonical re-encoding and all ordinary schema, arena, maximum and hash checks
remain intact. The final wholly owned value retains its settled capacity charge
through its last reference.

Tests compare frozen plans and native heap inventories, retain a final capture,
exercise budget refusal, every truncation, trailing bytes and ordinary hash-error
equivalence, and decode 64 plans with more than 1,024 measure fields each.
All 199 contract-IR tests and all seven scoped static checks passed.
This first-party section reader is composition groundwork. It does not admit
whole-bundle parsing or close the catalog/source/policy integration gap.
Public application behavior is unchanged, so the handbook needs no new interface
description for this internal decoder.

PR note: Package WP-824; Tier guarantee; Behavior: projection section admission;
Compatibility: unchanged bytes, hashes, checks and application interfaces;
Hazards/follow-ups: command/root assembly and complete production composition.

## Capability supersession and obsolete work

Registration compares canonical capability identity and revision across all five
families under discovery. A freshly authorized higher revision retires older
owners. A delayed lower revision refuses before changing any owner when a newer
registration remains known. Different plans, roles and partitions retain exact
data identities; this comparison grants no cross-policy reuse or authority.

Worker rebuilds require the actual captured slot. Private source captures retain
its existing pin through source destruction. Obsolete work refuses before source
observation and at bounded scan/replay, materialization and publication points.
Retirement preserves outstanding charges and pins. Background passes attempt at
most four retired-owner cleanups; all pins and the original 300-second retention
must expire first. Idle active registrations are not evicted by this cleanup.

Semantic tests cover all-family capability retirement, delayed lower revisions,
source observation and mid-replay cancellation, pin retention, minimum retention
and background owned-artifact cleanup. All 544 selected server tests passed,
including the real-daemon crash oracle; 72 tests were outside the selection or
already ignored. All six scoped static checks passed. Full catalog/source/policy admission and
exact plan supersession remain open; this is not qualification readiness.

PR note: Package WP-824; Tier guarantee; Behavior: exact capability supersession,
bounded obsolete work and background cleanup; Compatibility: unchanged authority,
ordering, formats and application interfaces; Handbook: Configuration;
Hazards/follow-ups: finish admission and plan supersession, full CI, qualification.

## Operator lifecycle accounting

The same process-graph accounting mutex now holds active/retiring owner counts,
unique pinned-owner count, distinct pin-guard count and cumulative released-owner
count, alongside reserved/owned bytes and accounting allocation counts. One
non-cloneable accounting token follows each native owner. A guard alias does not
create another pin; retirement preserves existing pins; the token releases only
after the owner's artifact state drops. Every update uses checked arithmetic and
publishes a complete counter snapshot. Underflow/overflow makes observations and
new budget admission fail closed rather than emitting misleading partial counts.
The finite accounting root now reserves 512 bytes, with a compile-time native
layout bound; the aggregate limit remains exactly 1 GiB.

The native worker reports fixed numeric `exact_provider_lifecycle` stderr samples
at startup, every due 60-second service-clock interval and shutdown. Invalid clock
or accounting observations report only `available=false`; repeated failures do
not generate a poll-frequency log stream. Values never include runtime identities,
paths or payloads. No metric label cardinality or SDK interface changes.

Tests prove actual unique native pin guards and aliases, retired-reference
retention, final owner/byte release, atomic overflow refusal, clock rollback/failure
and recovery, and the diagnostic line bound even at counter maxima. The existing
foreign-artifact test now asserts the active-to-retiring count change while
preserving every byte/reservation/allocation counter. The broader 715-test run
passed 713 tests; its two failures were fixtures hard-coding the prior 128-byte
accounting root. Updating those fixtures to use the reported fixed charge passed
all 12 focused server/projection regression tests and both failed tests on rerun.
All six final scoped static checks passed. Catalog/source/policy admission and
exact supersession remain open; WP-824 is not closed.

PR note: Package WP-824; Tier guarantee; Behavior: bounded operator lifecycle
samples; Compatibility: application interfaces, durable formats, authority and
retention unchanged; Handbook: Configuration; Hazards/follow-ups: finish remaining
admission/supersession integration, full CI and downstream qualification.

## Pressure retirement and claimed artifact cleanup

All five native registries preserve their 256-slot ceilings and exact keys.
Admission distinguishes capacity pressure from incomplete ownership evidence;
only capacity pressure may retire an eligible owner. Global pressure selects
by monotonic access ordinal, canonical family and canonical key. Family capacity
pressure selects inside that family. Checked 300-second minimum retention and
explicit query/cursor/worker/publication pins protect the existing obligations.
Capture succeeds before an access can extend its lease. Admission retries at
most the complete five-family inventory and otherwise returns existing typed
unavailable. No scan fallback or larger ceiling is introduced.

Selection closes new pins under discovery. A claimed retiring entry remains in
the map while its files are checked and removed outside the discovery lock;
this prevents reuse of the same checkpoint name before cleanup completes.
Failed custody keeps the tombstone and its charge. Successful cleanup detaches
the slot and destroys its final data references after unlocking discovery.
A permanently admitted 512-KiB single cleanup lane permits reclamation even
when the aggregate pool is full. Contention leaves phases untouched.

Each registration binds its private directory/name before capture. Initial
recovery records checked local cache objects once; later access refuses changed
objects rather than adopting them. Publication records ownership immediately
after creation and each no-replace publication step, including cancelled partial
work. Cleanup checks the complete recorded directory/name/file binding and
removes only owned selected/pending objects. Live descriptors pin claimed file
and directory inodes until the final claim drops. Directory custody retains the
original graph metadata charge after runtime destruction. Unknown/replaced
objects remain untouched. No authoritative storage, holds, envelopes or
application interfaces change.

Workers use admitted fixed-capacity key inventories and capture one active slot
immediately before work. Inventories hold no owner pins. A stack inventory was
rejected by the broader follower authorization test: it exceeded the existing
worker stack in a debug build. Its replacement reserves vector/wrapper capacity
before allocation and retains the charge through data destruction; stack limits
are unchanged.

Tests cover 1,024 text registrations, 512 registrations in each of five families,
protected saturation, retention/LRU ties, cleanup contention, full-budget census
recovery, removed-key worker capture, foreign-file tombstones, cancelled tracked
publication, final-directory charge retention and pinned unlinked inodes. The
checkpoint eviction/rebuild test uses an independent row/order/count oracle,
checks selected/pending deletion, ordinary Building state and fresh generations.
All 535 selected server tests passed, including the existing real-daemon crash
oracle; 72 tests were outside the selection or already ignored. All six scoped
static checks passed. Full CI remains required before package closure.

WP-824 stays open: full catalog/source/policy admission, exact supersession,
bounded obsolete-worker cancellation and operator diagnostics remain unfinished.
PR note: Package WP-824; Tier guarantee; Behavior: eligible pressure retirement
and owned derived-file cleanup; Compatibility: unchanged formats/SDKs and protected
continuation semantics; Handbook: Configuration; Hazards/follow-ups: complete
remaining composition and qualification before closing the package.

## Checkpoint custody before retirement

A failing semantic test demonstrated that pending-path symlinks were followed
and foreign bytes overwritten. Checkpoint operations now pin and recheck the
private directory and opened regular files, rejecting symlinks, unequal inode
identities, foreign ownership, permissive modes and unrelated hard links.
Root creation traverses bounded components through directory capabilities and
never follows symlinked ancestors. All temporary custody work is reserved before
path/capability construction; payload admission remains independent.

Publication creates a new private pending object, syncs it, removes only the
verified prior derived selection, links to an unoccupied name and syncs the
pinned directory. Restart recognizes only the exact two-name/same-inode
publication and completes its pending-name removal; an absent selection rebuilds
normally. Existing envelopes and slot/history checks remain intact. The existing
cap-std 4.0.2 dependency and feature set admitted by ADR-0265/0266 are reused.
Linux private custody fails closed elsewhere. Older permissive derived objects
need the documented stopped-daemon permission migration; no automatic chmod.

Tests cover foreign paths/links, replaced files/directories, relative paths,
private root creation, pre-I/O budget refusal and five abrupt-process exits.
All 517 selected server tests passed, including the existing daemon crash oracle
with complete row, idempotency and provenance assertions. All nine scoped static
checks passed, and the corrected relative-path fixture and final Clippy checks
passed. This is preparation for retirement: no artifact deletion
or slot-recycling readiness claim. PR note: Package WP-824 (open); Tier guarantee;
Compatibility: unchanged envelope/application interfaces; Handbook: Configuration;
Hazards/follow-ups: retirement, final-reference cleanup, full composition and CI.

## Long-pattern continuation ownership

Production long-pattern responses retain the actual charged immutable index,
generation/frontier and explicit registration pin. The service collects these
owners by compiled step ordinal and canonical partition hash, shares the frozen
collection across cursor clones, and returns each exact owner on resumption.
The private native owner must match the current registration owner, exact
request and pinned frontier. Current-head, minimum-epoch, history, policy,
authorization and complete participant-proof checks remain unchanged.

Each production response reserves its closed owner/charge allocations and its
contribution to collection growth before those allocations. Native layout bounds
cover small-vector rounding, geometric growth and the shared allocation and charge
roots. The builder combines same-pool reservations before vector growth, then
freezes the collection under one charge that outlives every participant destructor
and the vector allocation. Cancellation retains unpublished work through complete
vector destruction;
replacements receive newly admitted responses while prior published ownership
survives failure. Empty participant collections allocate no root. Expired cursor
purging precedes long-pattern preparation, and resolved requests retain their
independent references. No wire, query IR/module or durable format changes.

The new empty-index tests exposed an insufficient fixed builder allowance.
Long-pattern initialization now admits census scratch, inspects allocation-free
empty maps, and reserves their conservative tree and settlement layout before
mutation or immutable ownership allocation. Ordinary validation, settlement and
checkpoint bytes remain intact. Tests cover empty publication, charged captures,
ring advancement, exact owner/policy/frontier binding, expiry and immutable
collection growth, cancellation, shared final destruction and foreign-pool refusal.
All 795 affected projection/service/server unit tests passed (one existing skip),
and all six scoped non-test acceptance checks passed. Catalog admission, production
retirement and derived-file cleanup remain open; no run-readiness claim.

PR note: Package WP-824 (open); Tier guarantee; Behavior: retained native
long-pattern cursor participants and correctly admitted empty indexes;
Compatibility: existing formats, authority, ordering and freshness; Handbook:
Long Pattern and this page; Checks: 795 unit tests and six scoped static checks;
Hazards/follow-ups:
complete aggregate composition, retirement, cleanup and qualification.

## Native partition generation composition

A regression test exposed that finite-partition long-pattern batches assumed
all partitions rebuilt under one scalar generation. Independent rebuilds now
retain an ordered commitment to every constituent generation, including nested
merged observations, in the existing process-local epoch witness. The commitment
uses the existing partition-observation hash domain and streaming hasher; it
adds no allocation, hash domain, durable identity or serialized field. The
existing finite-route count bounds, schema/descriptor/history matching, complete
epoch intersection and work ceilings remain mandatory. Executor validation
compares the complete witness, so an equal representative generation cannot
substitute another constituent state. This is separate from cursor ownership.

Tests cover distinct generations at one valid epoch, substituted and nested
generations, a single unchanged observation and nonintersecting epochs. All 146
projection/executor tests and 746 service/server unit tests passed (one existing
unit skip). All six scoped non-test acceptance checks passed. The broader
cursor-retention run passed all 990 service/server tests with eight existing
skips before this additional proof repair. Neither provider retirement nor run
readiness is claimed.

PR note: Package WP-824 (open); Tier guarantee; Behavior: complete native
partition generation composition; Compatibility: existing query IR, module,
wire and durable bytes, scope, authorization, ordering and epoch intersection;
Checks: scoped acceptance and affected suites above; Handbook: Partition-Set
Queries, Long Pattern, Tokenized Text Search and this page; Hazards/follow-ups:
complete catalog admission, remaining cursor ownership, retirement and cleanup.

## Ranked cursor ownership

The native tokenized provider response now carries an admitted immutable epoch
owner through service cursor registration and continuation requests. That owner
retains the actual charged provider allocation and an explicit registration
capture pin, independently of the eight-entry discovery ring. Resumption checks
the private owner type, exact registration owner, current request identity,
epoch and generation before ordinary epoch negotiation and execution. Service
authorization and statistics/history checks continue at their existing points.

The native lease's two closed Arc allocations are admitted before construction;
its references retain their existing physical charges. Final destruction drops
provider data and the capture before the metadata charge. Continuations share
their existing owner without duplicating its physical allocation. Before each
exact-provider attempt, the existing checked service clock purges expired cursor
entries; captured requests independently retain their snapshots. Publication
still refreshes the ordinary 300-second expiry, and failed replacement retains
the prior published token. No token or durable encoding changes.

Tests cover expiry with a resolved capture, refreshed expiry, replacement and
cancellation, owner/epoch/policy mismatch, pressure retirement protection,
advancing the eight-epoch ring and final charged byte release. This completes
ranked tokenized cursor retention plumbing; complete catalog admission,
production retirement, derived artifact custody and other provider lifecycle
work remain open. No full aggregate-budget or rehearsal-readiness claim.

PR note: Package WP-824 (open); Tier guarantee; Behavior: actual native ranked
cursor retention; Compatibility: existing wire/durable formats, authorization,
ordering and public cursor options; Handbook: Tokenized Text Search and this
page; Checks: all 990 server/service Nextest tests passed (eight existing skips),
three tokenized provider tests, two cursor lifetime tests, and all six scoped
non-test acceptance checks; Hazards/follow-ups: remaining catalog,
retirement, cleanup and qualification gates.

## Fresh provider rebuild generations

All five production rebuild paths now share one runtime generation issuer,
independent of rebased access ordinals and discovery entries. Every admitted
rebuild consumes a fresh generation, including abandoned candidates. Checked
issuance advances beyond both its own floor and the prior selected generation;
validated checkpoint recovery raises the floor before reuse or rebuilding.
Ordinary checkpoint identity, history, plan/policy/partition, source and
publication checks remain in place. The existing registry metadata allowance
explicitly includes the issuer's closed inline layout.

Tests cover restore floors, abandoned work, concurrent issuance, an explicit
two-thread observation/issuance schedule, lower observations and exhaustion.
The server unit suite passed 497 tests with one existing skip. Before the final
run, parallel unit tests exposed cross-test saturation by a production-budget
negative test. That test now runs its unchanged assertions in a bounded child
process, preserving production accounting and preventing interference. The
preceding process-isolated Nextest run also passed 497 tests with one existing
skip. All six scoped non-test acceptance checks passed.

This is a prerequisite for retirement. Discovery eviction, protected-cursor
eligibility, fresh artifact custody, final-reference cleanup and complete catalog
admission remain unfinished. Existing per-family slot refusal remains enabled.
No aggregate-enforcement, full CI or rehearsal-readiness receipt is claimed.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal fresh rebuild
issuance and isolated production-budget regression assertions; Compatibility:
existing generation type, checkpoint format, authority, ordering and public query
semantics; Checks: server unit/Nextest tests and scoped acceptance; Handbook: this
page; Hazards/follow-ups: complete catalog and production retirement lifecycle.

## Admitted structural schema construction

A hidden first-party fragment reader now reserves before ordinary structural
schema decoding. Borrowed inspection covers records, value-type boxes, keys,
expression arenas, invariants, indexes, events and their partitions/policy
anchors, enums, aggregates, relationships, unique keys, deletion policies and all
existing vector, secret, text and long-pattern extensions. Native layout bounds
include small-vector reserve rounding, constructor name/field trees, expression
reachability, simultaneous global index/invariant/entity/ownership forests and
map-builder input pairs. Relationship traversal accounts for the capped product
of two DAGs, including the rejecting insertion, and growing pending vectors.

Constructor work is conservatively summed by its actual allocation domains;
canonical output is a later phase. Ordinary constructors retain every schema,
reference, key, partition, enum, integrity and format check. Canonical comparison
rejects reordered input even when ordinary constructors sort it. Final settlement
retains one immutable charged schema; encoded input and complete executable
catalog identity remain separate.

Tests cover all 13 frozen schemas, native retained and simultaneous validation
capacity, 4,096 entities/fields/variants, relationship traversal, all long-pattern
operators, truncation, invalid names/operators, noncanonical fields, trailing
bytes, ceilings, pressure, overflow and final captures. All 467 contract IR and
contract compiler tests passed with no skips. All seven scoped non-test acceptance checks passed. The preceding fragment receipt was corrected from an expected
461-test count: its actual contract compiler run passed 460 tests, and the
separate query compiler run passed 291 tests.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal admitted
structural decoding; Compatibility: existing formats, constructors, identity,
authorization and application interfaces; Checks: contract IR/compiler tests and
scoped acceptance; Handbook: this page; Hazards/follow-ups: executable and whole
bundle composition, retained catalogs, production retirement and cleanup remain
unfinished. No aggregate-enforcement or rehearsal-readiness claim.

## Admitted key and expression fragments

Hidden first-party fragment readers reserve before decoding existing V1/V2 key
schemas and expression arenas in supported IR versions. Borrowed inspection
accounts for component and enum vectors, embedded key boxes, nested type boxes,
canonical constant buffers and the simultaneous expression input, checked-node
and depth vectors. Constructors move private values into the result; canonical
verification follows destruction of their temporary vectors. Final settlement
retains one charged immutable fragment through the last capture.

Tests preserve keys and command arenas from all 13 frozen bundles, cover nested
enum and ordered-byte keys, every expression tag, nested constants, legacy version
gates and the maximum 131,072-node arena. Native witnesses cover retained data,
constructor overlap and exact canonical output. Truncation, semantic failures,
noncanonical enum order, trailing bytes, pressure, overflow and final-reference
release fail closed. All 460 contract IR/contract compiler tests and all seven scoped
non-test acceptance checks passed. Whole-bundle construction, catalog assembly, production
retirement and cleanup remain open; this does not establish run readiness.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal admitted key
and expression parsing; Compatibility: existing encodings, constructors, hashes
and application interfaces; Checks: contract IR/compiler tests and scoped
acceptance; Handbook: this page; Hazards/follow-ups: compose complete catalog
admission and finish the production lifecycle before qualification.

## Admitted row-policy parsing and constructor overlap

A hidden first-party reader covers the existing V1 row-policy section. Borrowed
inspection bounds fact/policy/rule/node vectors, names, indexed-probe arguments,
canonical constants and nested value-type boxes before ordinary decoding. It
admits both retained fact maps, private map key/type copies and map-builder input
pair vectors. Reconstruction can retain original and replacement policy vectors
and one pair of rule vectors while validating one rule; the reservation covers
that overlap and the largest type/disjunction scratch vectors with deep type
copies. Construction scratch is destroyed before exact canonical verification.

The existing row-policy decoder and constructors retain their operation, type,
principal-fact, node, rule, dependency and schema checks. Constant decode sizing
reuses the established borrowed canonical-capacity inspector; ordinary semantic
decoding remains required. The common B-tree layout allowance now accepts key and
value types so map layout matches the existing ownership inspector; prior set
bounds are unchanged. Final settlement owns one immutable charged policy catalog.
No policy encoding, authorization semantics, query guarantees or application
interfaces change. Structural schema, encoded input ownership and complete bundle
identity remain separate.

Tests cover frozen catalogs and a populated catalog with list facts, principal
fact references, list/string constants and membership. Independent native
witnesses cover both maps, the pair-input vector, replacement vectors, cloned
rule types, retained data and canonical output. Every value-type tag and record
owner is covered, including 31 owned boxes and the rejected depth boundary.
Rejections cover every truncation point, unsupported versions, fact ceilings,
invalid constants/types, reordered facts, trailing bytes, pressure, overflow and
final capture release. All 455 contract IR and compiler tests passed with no skips.
All seven scoped non-test acceptance checks passed, including formatting, Clippy
with warnings denied, handbook, downstream adapters, scope and source guards.

WP-824 remains open. Whole-bundle construction and composition, retained catalog
assembly, parent lineage proofs, production capture, source pins, retirement,
cleanup and qualification remain unfinished. No aggregate-enforcement or full
CI receipt is claimed.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal admitted
row-policy construction and immutable ownership; Compatibility: existing policy
encoding, ordinary authorization/dependency checks and application interfaces;
Checks: contract IR/compiler tests and scoped acceptance; Handbook: this page;
Hazards/follow-ups: complete the remaining construction and provider lifecycle.

## Admitted generated-schema registry decoding

A hidden first-party reader covers the existing generated-schema registry section
against already constructed IR. Borrowed inspection checks bounded framing and
counts without copying stored JSON. The existing source visitor and schema sizer
then bound the exact artifact vector, every retained canonical string, and the
largest JSON construction/render workspace. One reservation covers all prior
artifacts simultaneously with the next generation and final ownership inspection;
it precedes ordinary decoding and generation.

The existing decoder remains the authority for registry order, keys, canonical
JSON bytes and schema hashes. It never accepts stored JSON without regeneration.
Final settlement retains one immutable charged vector. No artifact format, hash,
ordinary decoding behavior or application interface changes. Input ownership,
structural/executable IR construction and complete bundle identity are separate.

Tests preserve every artifact in all 13 frozen bundles and prove exact retained
vector capacity plus its composition with the established native JSON-tree/render
bound. Rejections cover each truncation point, count/ceiling, key/JSON/hash and
order mismatches, a late mismatch after earlier output was retained, trailing
bytes, pressure before sizing and generation, overflow and final capture release.
All 450 contract IR and compiler tests passed with no skips. All seven scoped
non-test acceptance checks passed, including formatting, Clippy with warnings
denied, handbook, downstream adapters, scope and source guards.

WP-824 remains open. Whole-bundle composition, remaining constructors, retained
catalog assembly, parent lineage proofs, production capture, source pins,
retirement, cleanup and qualification are unfinished. No aggregate-enforcement or
full CI receipt is claimed.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal admitted registry
decoding with retained output and generation accounting; Compatibility: existing
artifact formats, hashes, canonical bytes and application interfaces; Checks:
contract IR/compiler tests and scoped acceptance; Handbook: this page;
Hazards/follow-ups: complete whole-bundle construction and provider lifecycle.

## Admitted complete ledger validation of constructed IR

Complete ledger validation now has a hidden first-party admission path. Borrowed
inspection accounts for required namespace vector growth and owner paths, the
expected executable identity vector and private name/path copies, and the
history/alias identity tree with its private copies. It reserves for their
simultaneous lifetime and one temporary namespace owner-vector clone before
ordinary validation constructs them. Checked arithmetic refuses overflow.

A common borrowed visitor supplies the existing expected-identity order to both
sizing and ordinary validation, covering entities, fields, invariants, indexes,
text and long-pattern indexes, events, enums, aggregates, commands, outcomes and
projections. The ordinary text-index walk now borrows its checked historical name
instead of cloning an intermediate string. The closed global namespace list is
shared with sizing. No canonical bytes, lookup semantics, durable format or
application interface changes.

All 13 frozen bundles revalidate unchanged. Native witnesses cover required,
expected and history/alias forests while simultaneously retained, with a separate
preadmitted ownership-inspection allowance. Semantic tests cover missing required
states, ordering, active-identity mismatch, invalid complete history, missing alias
targets, collisions, pressure before sizing and construction, overflow and full
workspace release. All 446 contract IR and compiler tests passed with no skips.
All seven scoped non-test acceptance checks passed, including formatting, Clippy
with warnings denied, handbook, downstream adapters, scope and source guards.

This admits validation of already constructed IR. The section reader above admits
parsing separately; a future whole-bundle reader must hold every input and
construction reservation across the full composition. Remaining constructors,
retained catalog assembly, parent lineage proofs, production capture, source pins,
retirement, cleanup and qualification remain open. WP-824 is not complete and no
aggregate-enforcement or full CI receipt is claimed.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal admitted complete
ledger validation workspace and shared borrowed traversal; Compatibility: existing
ledger semantics, canonical bytes and application interfaces; Checks: contract
IR/compiler tests and scoped acceptance; Handbook: this page;
Hazards/follow-ups: finish the whole-bundle composition and provider lifecycle.

## Admitted lineage-ledger section parsing

A hidden first-party reader covers the existing V1/V2 ledger section. Borrowed
inspection bounds allocation states, contiguous entry vectors, alias vectors,
owner-path vectors and source names before ordinary decoding. It includes the
single temporary owner-vector clone used to compare an identity's allocation
namespace. Construction scratch is destroyed before canonical verification, so
those temporary phases share a peak allowance. Checked arithmetic refuses
overflow. Final settlement retains one immutable charged ledger.

The reader preserves ordinary section checks for versions, namespace and state
tags, valid owner paths, contiguous IDs, allocation-state order, complete-history
maxima, and aggregate entry and alias ceilings. Canonical verification uses a
pre-admitted exact output buffer. Schema/command/projection validation, required
allocation states, active identities, aliases against their targets, parent
lineage history and complete bundle identity remain separate; this reader is
not a validated catalog or a new durable format. Encoded input ownership also
remains separate.

Tests cover frozen sections, scoped owners, active entries and tombstones, V2
aliases, native decoded/temporary/canonical capacity witnesses, the 262,144-entry
ceiling, aggregate history overflow, alias ceilings, duplicate allocation states,
malformed 255-component owner paths, every truncation point, trailing bytes,
invalid IDs, mismatched namespaces and maxima, unsupported versions and tags,
reserved names, pressure, overflow and final capture release. All 442 contract IR
and compiler tests passed with no skips. All seven scoped non-test acceptance
checks passed, including formatting, Clippy with warnings denied, handbook,
downstream adapters, scope and source guards.

WP-824 remains open. Whole-bundle construction, complete ledger validation,
retained catalog assembly, production capture, source pins, retirement, cleanup
and qualification still require implementation. No aggregate-enforcement or
full CI receipt is claimed.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal admitted ledger
section parsing and immutable output ownership; Compatibility: existing ledger
encoding, ordinary section checks, limits and application interfaces; Checks:
contract IR/compiler tests and scoped acceptance; Handbook: this page;
Hazards/follow-ups: complete bundle and catalog construction and provider lifecycle.

## Admitted MCP registry parsing and shared lineage ownership

A hidden first-party reader admits the existing MCP registry section before
ordinary entry and registry construction. Borrowed inspection bounds entry and
name buffers, lineage conversion and shared destruction bookkeeping, the borrowed
collision-name tree, and the largest normalization workspace. The latter covers
two normalized segments, the formatted complete name and the temporary tool-name
clone during registry validation. Canonical verification uses an admitted exact
buffer after constructor scratch has been destroyed. Settlement attaches lineage
ownership to its actual final destruction; an escaped lineage remains charged
when the enclosing registry is dropped.

Registry completeness validation now counts and traverses borrowed application
commands, preserving reimport exclusion, lineage, count and exact command/name
binding checks. It no longer builds a temporary command-reference vector. The
ordinary constructors, derivation, collisions, limits, versions and canonical
bytes remain authoritative; MCP visibility and application interfaces are
unchanged. Encoded input ownership, complete bundle identity and catalog assembly
remain separate from this section reader.

Tests cover frozen registries, empty registries, 4,096-entry capacity witnesses,
maximum normalization inputs, lineage final destruction, saturated/partial
pressure, every truncation point, trailing bytes, hostile counts, unsupported
versions, zero IDs, collisions, duplicate IDs, wrong derivation, reordered entries,
arithmetic overflow and exact completeness/binding errors. All 437 contract IR
and compiler tests passed with no skips. All seven scoped non-test acceptance
checks passed, including formatting, Clippy with warnings denied, handbook,
downstream adapters, scope and source guards.

WP-824 remains open. Remaining bundle construction, retained catalog assembly,
production capture, source pins, retirement, cleanup and qualification are not
covered by this receipt. No aggregate-enforcement or full CI claim is made.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal admitted MCP
registry construction and final shared lineage ownership; Compatibility: existing
registry encoding, names, visibility, semantic checks and application interfaces;
Checks: contract IR/compiler tests and scoped acceptance; Handbook: this page;
Hazards/follow-ups: finish the remaining construction and provider lifecycle.

## Admitted compatibility-report parsing

A hidden first-party reader admits the existing compatibility-report section
before entry construction. Borrowed inspection accounts for the entry vector,
path strings, retained stable-path vectors and the parser's temporary borrowed
path-part vector, including malformed slash-dense paths. Sorting remains in place.
The empty-successor synthesis and its vector growth are also admitted before the
ordinary constructor runs. Checked arithmetic refuses overflow.

The reader preserves the ordinary finding-code, path, duplicate, overall-class
and genesis/successor checks. A pre-admitted exact output buffer verifies canonical
section bytes before settling one immutable charged report. Complete bundle
identity, encoded input ownership and parent compatibility comparison remain
separate; this does not introduce an encoding, application interface or migration
policy.

Tests cover frozen reports, all stable-path shapes, 4,096-entry reports, native
capacity witnesses, maximum malformed paths, truncation, trailing bytes, hostile
counts, overall-class mismatch, reordered entries, noncanonical empty successors,
pressure before sizing and construction, arithmetic overflow and final captures.
All 432 contract IR and compiler tests passed with no skips. All seven scoped
non-test acceptance checks passed, including formatting, Clippy with warnings
denied, handbook, downstream adapters, scope and source guards.

WP-824 remains open. Whole-bundle admission, catalog assembly, production capture,
source pins, retirement, cleanup and qualification still require implementation.
No aggregate-enforcement or full CI receipt is claimed.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal admitted
compatibility-section parsing and output ownership; Compatibility: existing
encoding, semantic checks, canonicality and application interfaces; Checks:
contract IR/compiler tests and scoped acceptance; Handbook: this page;
Hazards/follow-ups: complete the remaining provider construction and lifecycle.

## Admitted workflow parsing and constructor workspace

A hidden first-party reader covers the existing workflow section of supported
V2 through V26 IR. Borrowed inspection bounds workflow/transition/state vectors
and string buffers before decoding. Constructor admission covers transition-edge
sets, the catalog's entity set, one enum-variant set at a time, lease-field vector
growth and ownership overhead. The bound accounts for phase overlap: per-workflow
edge sets are destroyed before catalog validation, and constructor scratch is
destroyed before canonical verification. Overflow refuses admission.

The reader uses the ordinary workflow, transition, lease and catalog constructors.
Whole-bundle parsing now moves its parsed workflow vector into bundle validation
instead of deep-copying that intermediate catalog; the validation remains.
The admitted section reader then verifies exact canonical section bytes with a pre-admitted output buffer
and settles one immutable charged catalog. No section format, whole-bundle format,
language construct, schema semantics or application interface changes. Encoded
input ownership, structural schema and complete bundle identity remain separate.

Tests cover current frozen sections, populated and empty catalogs, fenced leases,
legacy V2/V3 sections before initial-state metadata, every truncation point,
trailing bytes, hostile counts, invalid lease duration, reordered transitions,
unsupported IR, saturated/partial pressure, arithmetic overflow and final captures.
Independent native capacity inspections cover decoded data, constructor sets and
canonical verification. All 428 contract IR and compiler tests passed with no
skips. All seven scoped non-test acceptance checks passed, including formatting,
Clippy with warnings denied, handbook, downstream adapters, scope and source
guards.

This admits one parser/constructor domain. The complete bundle reader, remaining
constructor and write-evidence workspace, retained catalog assembly, production
capture, source pins, retirement, cleanup and qualification remain unfinished.
WP-824 is open; no aggregate-enforcement or full CI receipt is claimed.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal admitted workflow
section parsing/validation and immutable output ownership; Compatibility: existing
encodings, ordinary constructors, limits and canonicality; Checks: tests above
and scoped acceptance; Handbook: this page; Hazards/follow-ups: complete the
remaining bundle construction and provider lifecycle before qualification.


## Admitted schema registry validation

A common borrowed source visitor preserves the existing entity, event,
command-input, command-outcome and projection artifact order. Provider registry
validation now admits generation of each expected artifact before comparing its
key, canonical JSON and hash, and destroys that temporary artifact before moving
to the next. Count/order/mismatch checks retain their ordinary meanings.

Projection artifacts borrow group value types directly from the checked plan.
The ordinary slice factory and the internal plan source share the same borrowed
sizing and JSON tree builders, owner validation, rendering and hashing. This
removes the registry visitor's temporary vector and deep value-type clones without
changing its artifacts or exposing an application budget selector.

All 13 frozen compiler bundles retain their bytes and validate unchanged. Native
JSON capacity witnesses also cover the borrowed projection source. Tests cover
refusal before sizing and after sizing fits, final temporary release, a late
registry mismatch after earlier artifacts have been generated, and an invalid
count rejected before generation. All 423 contract IR and compiler tests passed
with no skips. All seven scoped non-test acceptance checks passed, including
formatting, Clippy with warnings denied, handbook, downstream adapters, scope
and source guards.

This is registry validation of already constructed IR. Parsing, constructor and
write-evidence workspace, retained catalog assembly and production capture remain
unadmitted. Source pins, retirement, cleanup and qualification remain open;
WP-824 is not closed and this is not an aggregate-enforcement or full CI receipt.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal admitted schema
registry validation and borrowed projection inputs; Compatibility: existing
canonical artifacts, registry order, semantic checks and application interfaces;
Checks: tests above and scoped acceptance; Handbook: this page;
Hazards/follow-ups: finish whole-bundle construction and the remaining lifecycle.


## Admitted command and projection hash validation

Existing enum-dependency traversal now has an exhaustive private inventory for
its enum, visited-record, entity and event sets. Provider hash validation admits
all four sets before traversal. Its bound uses the existing conservative native
B-tree layout, schema registry sizes, command outcome roots and possible
binding/read/instruction members. The helper borrows an already validated bundle;
it does not claim to admit arbitrary undecoded plans.

After dependency traversal, a borrowed counting pass sizes the exact command or
projection preimage. The helper reserves that output before allocating it and
retains dependency work until both the preimage and dependency sets are destroyed.
Ordinary and admitted paths use the same version selection, semantic encoding,
enum closure order and hash domains. Existing hash mismatch classifications remain.

All individual hashes in the 13 frozen compiler bundles validate unchanged.
Independent complete-forest capacity inspections fit the prospective reservation.
Tests cover saturated refusal before traversal, refusal after either family's
forest fits but its output cannot, complete release, and checked size overflow.
All 421 contract IR and compiler tests passed with no skips. All seven scoped
non-test acceptance checks passed, including formatting, Clippy with warnings
denied, handbook, downstream adapters, scope and source guards.

IR parsing, constructor/write-evidence checks, schema registry validation and
production catalog capture remain unadmitted. Source pins, retirement, cleanup
and qualification remain open. WP-824 is not closed.

PR note: Package WP-824 (open); Tier guarantee; Behavior: admitted internal plan
hash traversal and encoding workspace; Compatibility: existing bytes, hashes,
validation and application interfaces; Checks: scoped tests and acceptance;
Handbook: this page; Hazards/follow-ups: finish complete bundle construction and
provider lifecycle before qualification. No aggregate-enforcement or full CI claim.


## Admitted canonical bundle and root-hash encoding

A borrowed counting writer now uses the ordinary canonical encoding traversal
without building an output buffer. Existing canonical constants append directly
into the enclosing writer; the same borrowed length validator preserves their
bounds and ordinary IR error mappings. Admitted bundle encoding reserves one exact
output allocation and settlement scratch before construction. Its immutable output
keeps the private byte charge through the final capture.

Plan-root hashing sizes the structural schema, structural hash preimage and root
preimage before allocating any encoding buffers. Its reservation covers the
structural bytes plus the larger overlapping preimage phase, using actual native
buffer lengths and ownership overhead. The structural preimage is destroyed before
the root buffer is allocated. Row-policy framing uses the same checked standalone
encoding traversal without another row-policy byte buffer. Ordinary and admitted
root hashing share that framing; ordering and hash domains are unchanged.

All 13 frozen compiler bundles preserve canonical bytes and root hashes. Checks
cover final output captures, refusal before sizing and after sizing fits, constant
encoding and error equivalence, integer overflow, observed simultaneous native
buffer capacities, and an independent reconstruction of the original root
preimage. Removing a projection changes the root identically in ordinary and
admitted hashing. All 417 contract IR and compiler tests passed with no skips.
All seven scoped non-test acceptance checks passed, including formatting, Clippy
with warnings denied, handbook, downstream adapters, scope and source guards.

This covers encoding workspace for already constructed first-party IR. IR parsing,
command/projection validation scratch and production catalog capture remain
unadmitted. Source pins, provider retirement, artifact cleanup and qualification
also remain open. WP-824 is not closed; no aggregate-enforcement or full CI claim.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal admitted bundle
output and plan-root encoding workspace; Compatibility: existing bytes, hashes,
ordering, validation errors and application interfaces; Checks: tests above and
scoped acceptance; Handbook: this page; Hazards/follow-ups: finish whole-bundle
construction and the remaining lifecycle before qualification.


## Admitted generated-schema workspace

A closed borrowed first-party source covers existing entity, event, command-input,
command-outcome and projection-result artifacts. It reserves bounded numeric and
pattern sizing work before the existing size preflight, then reserves JSON-tree,
container-growth, rendering and settlement work before ordinary generation. The
workspace bound uses actual JSON/String layouts and the ownership inspector's
existing B-tree layout allowance; it includes sparse roots, concurrent old/new
vector buffers, growing strings and output encoding. It is checked for overflow.

Ordinary and admitted generation share the same tree builders, input validation,
canonical renderer and hash. The returned immutable artifact retains its private
string charge through the final capture. All artifacts in the 13 frozen compiler
bundles match exactly, and an independent capacity inspection compares their trees
and rendered buffers against the prospective workspace bound. Further checks cover
sparse objects, vector growth, nested arrays, saturated refusal, refusal after
sizing fits, arithmetic overflow and unchanged wrong-owner rejection. All 139
contract IR tests, all 271 compiler tests and all seven scoped non-test acceptance
checks passed, including formatting, Clippy with warnings denied, handbook and
downstream adapter checks.

This prepares catalog validation scratch. Contract IR parsing and production
catalog/lineage/materialization capture remain unadmitted; the new helper does not
claim that an entire bundle decode is admitted. Source pins, provider retirement,
cleanup and qualification also remain open.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal admitted schema
artifact construction; Compatibility: existing artifact bytes, hashes, semantic
checks and application interfaces; Handbook: this page; Hazards/follow-ups: finish
whole-bundle construction and the remaining provider lifecycle before qualification.


## Admitted permanent catalog migration evidence

Existing migration-record and predecessor-retirement wire roles now support the
closed caller-admitted decoder. Semantic reconstruction reuses their ordinary
mappers. Exhaustive inventories cover the retained record, opaque checksum,
backup name, principal and complete joined edge; the actor's existing shared
lifetime proof keeps its charge through an escaped principal capture.

Snapshot migration reads admit bounded join copies before construction and join
both records on the same captured root. They preserve predecessor-key identity,
operation/artifact/sequence cross-links and ordinary corruption classification.
Missing retirement evidence remains absent; required missing migration evidence
remains corruption. The private join allowance has a compile-time bound using
actual root size and the three copied buffers' existing limits.

Tests preserve both framings and reject invalid terminal counts. Snapshot tests
cover budget saturation, final principal captures, a historical pin, substituted
parent/candidate/sequence, an absent operation and incompatible framing. All 613
selected types, Protobuf, storage API and catalog storage library tests passed;
793 tests were outside the selection or already ignored. All eight scoped
non-test acceptance checks passed, including generated artifacts and Clippy with
warnings denied.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal admitted
permanent migration evidence and captured joins; Compatibility: existing durable
roles, bytes, ordinary constructors, cross-links and application interfaces;
Handbook: this page; Hazards/follow-ups: contract IR construction, production
catalog/lineage/materialization capture, source pins, retirement and cleanup remain
open. No aggregate-enforcement, full CI or rehearsal claim.


## Admitted catalog records on a captured storage root

Hidden first-party snapshot helpers now reconstruct active pointers and immutable
opaque bundles directly from borrowed bytes on the same captured metadata root.
They reserve bounded key/handle work before opening tables and delegate allocation
and durable verification to the admitted wire decoder. Active reads still require
a matching stored bundle; bundle reads still require the exact requested lineage
and version. Absence retains the ordinary meaning. No newer root is reopened.

Tests deploy a successor after taking a pin and compare ordinary and admitted
reads on both roots. They check escaped lineage ownership, saturated refusal,
release after a provisional pointer fits but its required bundle does not, and
checksum-valid wrong-identity corruption. A previously captured valid root remains
valid after that corruption is committed. All 813 storage library tests passed
with one existing skip; all six scoped non-test acceptance checks passed,
including formatting, Clippy with warnings denied, scope and handbook checks.

These are opaque storage-record helpers. Production provider catalog assembly,
IR decoding, migration evidence, lineage/materialization scratch and ownership of
source pins still require admission. Retirement and artifact cleanup also remain
open; this slice does not complete aggregate accounting or qualify the rehearsal.

PR note: Package WP-824 (open); Tier guarantee; Behavior: admitted catalog storage
reads on the existing snapshot; Compatibility: ordinary key/pointer checks,
durable records and application interfaces preserved; Handbook: this page;
Hazards/follow-ups: complete production catalog capture and the remaining lifecycle.


## Admitted catalog wire reconstruction

Existing active-pointer and opaque bundle records now support the closed,
caller-admitted provider decoder. Borrowed framing inspection precedes generated
message reconstruction; independent admitted scratch verifies the same V1/V2
framing, checksum and canonical encoding. Semantic reconstruction uses the
ordinary lineage, version and fixed-hash validators. Settlement accounts for
private bundle bytes and the lineage allocation through its actual final reference.

These helpers decode existing wire roles only. They neither decode contract IR
nor change durable bytes, catalog validation or application interfaces. Production
snapshot integration, IR construction and lineage/materialization scratch remain
open, together with provider retirement and artifact cleanup.

Tests compare ordinary and admitted values under both framings, retain an escaped
lineage after its wrapper is dropped, refuse construction under pressure and
preserve semantic rejection of malformed versions and hash lengths. All 761 Protobuf and storage API tests and all eight scoped non-test
acceptance checks passed, including generated artifacts, formatting and Clippy
with warnings denied.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal admitted catalog
wire reconstruction; Compatibility: existing roles, formats and semantic checks;
Handbook: this implementation page; Hazards/follow-ups: connect captured snapshot
reads and complete IR admission and retirement before qualification.


## Settlement of mixed catalog ownership

Trusted construction can now settle one previously admitted maximum into an
immutable value with private and shared ownership. The bounded census and
attachment plan use scratch split from that allowance. Settlement checks all
existing owners and the complete remaining charge before attaching anything;
it never grows the allowance. Shared owners must identify the exact allocation
and graph budget, including a concurrent attachment winner.

The returned capture keeps its private root charge until final destruction.
Each shared data allocation independently keeps its charge through its actual
last reference. Cancellation and failed settlement destroy the candidate before
releasing the remaining construction work. A catalog fixture proves publication
identity is preserved and an escaped enum table stays charged after both wrapper
and ordinary catalog captures are gone. Further tests prove shared deduplication,
refusal without destruction proof, refusal of foreign owners, and allowance
refusal before partial attachment.

This settles admitted construction; it does not establish the maximum for the
existing ordinary catalog decoder. Borrowed catalog framing, decoder and lineage
scratch admission, source assembly and retirement remain open.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal mixed ownership
settlement; Compatibility: existing wholly owned settlement still refuses shared
payloads, and catalog and wire semantics remain unchanged; Checks: final scoped
validation: all 353 catalog and types tests and all six scoped static checks
passed, including formatting, clippy with warnings denied and handbook;
Hazards/follow-ups: complete production catalog capture
admission. Existing handbook provider refusal documentation applies; no application
or operator controls are added.

## Catalog shared ownership foundation

The publication, validated bundle, lineage proof, active command-plan map,
individual command plan and enum-name table now expose exhaustive capacity
inventories and typed final-destruction ownership. Data fields precede their
ownership markers. Deep value clones create fresh markers; shared captures keep
the original marker. Boxed lineage arrays account for both element storage and
nested allocations. Active pointer and executable-reference inspection preserves
their existing structural identities.

The enum-name carrier remains an immutable shared table with the existing alias,
lookup operations and pointer identity. Its first-party backing value now owns
its marker; service constructors and four generated adapter test constructors
convert ordinary maps into that value. The frozen adapter source and its digest
were regenerated with the existing generator. SDK methods, transport schemas,
enum wire values and durable encodings are unchanged; first-party Rust code
constructing the table from a map uses `.into()`.

Tests retain a resolved plan and enum table after dropping both catalog captures,
then compare the exact remaining charges with the original live allocations.
Dropping the last data reference releases those charges. Barrier-controlled
competing attachments charge each actual allocation once. Pressure after scratch
admission refuses the complete attachment allowance before any marker is attached,
and retry succeeds after the competing reservation is dropped. These tests cover
shared attachment and lifetime, not pre-admitted catalog decoding.

Catalog read construction, lineage validation scratch, materialization scratch
and production provider capture admission remain open. Ordinary catalog reads
retain their existing validation and refusal behavior. WP-824 is still open.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal catalog ownership
inventories and lifetime markers; Compatibility: unchanged canonical and wire
semantics, with the internal Rust enum-table construction adjustment above;
Checks: all 1,600 tests across the six affected crates passed; all nine scoped
static checks passed, including clippy with warnings denied, generated artifacts,
version topology, handbook and downstream adapters; Hazards/follow-ups: complete
pre-allocation catalog capture admission and provider retirement. Existing handbook
provider refusal documentation applies; no application or operator control changes.

## Contract bundle ownership inventories

Complete non-durable inventories now cover the decoded contract bundle and
106 previously uncovered IR types reachable through schemas, command plans,
expressions, workflows, policies, projections, compatibility findings, stable
identity ledgers and generated artifacts. Exhaustive field patterns and enum
matches make added ownership fail compilation until accounted for. Existing
bounded traversal, checked capacity arithmetic and shared lineage deduplication
apply. Embedding metadata includes both owned strings; the closed metric and
text result model remain inline values.

Tests inspect all thirteen canonical compiler bundle fixtures under admitted
scratch, preserve their exact content and bytes, and prove that deep clones
account for their own capacities while shared lineage allocations deduplicate.
Separate tests increase unused canonical and command-vector capacity without
changing content and require the inventory to count it. A clone can shrink
vector capacity, so the ownership oracle measures both actual allocations.

These inventories prepare catalog admission; they do not yet admit catalog
construction or establish final-reference ownership of its private shared roots.
WP-824 remains open, including retirement, cursor protection and derived cleanup.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal complete IR
ownership inspection; Compatibility: unchanged canonical formats and semantics;
Checks: all 360 contract-IR and types tests and all seven scoped static
acceptance checks passed, including clippy with warnings denied, handbook and
downstream adapters; Hazards/follow-ups: catalog construction
and shared lifetime accounting remain open. This adds no application or operator
behavior; existing handbook provider refusal documentation remains applicable.

## Checkpoint encoding admission

All six physical formats now admit checkpoint encoding before allocating their
temporary scalar, gram, canonical-output and wire buffers. Capacity inspection
runs under admitted scratch, and a conservative construction allowance precedes
the unchanged encoder. The returned immutable byte allocation retains its charge
through the final capture. Semantic tests compare every format's exact ordinary
bytes, and a pressure test proves refusal after sizing leaves the provider intact.

Production persistence uses the same graph budget for encoded state, the
long-pattern epoch envelope, the activation frame, and temporary filesystem path
buffers. Each outer buffer is admitted before copying an already charged inner
buffer. Data and path values are destroyed before their corresponding allowances;
file write, sync, rename, directory sync, and test hooks preserve the existing
publication sequence. Activation tests verify byte compatibility, strict decode,
pressure refusal, invalid-key refusal, and final-reference release.

Source and catalog assembly, policy and mutation scratch, query results, cursor
and epoch protection, retirement and derived artifact cleanup remain open. This
completes checkpoint encoding integration, not aggregate enforcement or WP-824.

Validation: all 120 selected projection/provider conformance tests passed
(524 excluded by selection), including exact-byte encoding assertions for all
six physical formats. All 28 adapter tests and six scoped static acceptance
checks passed, including formatting, clippy with warnings denied, handbook,
scope, file-size, and panic allowances.

PR note: Package WP-824 (open); Tier guarantee; Behavior: admit internal checkpoint
encoding and persistence buffers; Compatibility: existing checkpoint bytes and
publication sequence; Hazards/follow-ups: finish source ownership and retirement
before qualification. Existing handbook transient-unavailable behavior applies;
no application or operator control changes.

## Approved catalog capacity scope extension

Additional allowed path: `crates/riffdb-catalog/src/**`.
Maintainer approval: in session, 2026-10-02, “i approve”, accepting the
concrete catalog scope proposal below.
Provider source capture invokes `ActiveCatalogSnapshot::read`, which owns a
private publication, validated bundles and enum-name tables, a complete lineage
materialization proof, and a shared command-plan map. Those private catalog
roots currently provide neither an exhaustive capacity inventory nor typed
final-destruction ownership, so server-side inspection cannot complete their
accounting through the public borrowed accessors.

The concrete scope is exhaustive private catalog inventories, typed shared
ownership bookkeeping, admitted provider catalog capture, and their tests.
Use the existing types budget and approved contract-IR capacity helpers; retain
all activation, lineage, migration and materialization checks, catalog cache and
lineage ceilings, and existing ordinary read behavior. Admission pressure must
refuse a provider capture before unadmitted allocation. No durable encoding,
dependency, application interface, ordering, authorization or freshness change
is proposed. The implementation must prove shared deduplication and release
through actual final data references, preserve canonical bytes and catalog
semantics, and test pressure, malformed evidence, cancellation and concurrent
captures. The scope widening is recorded before catalog implementation changes.

## Predicate recovery and production ownership

V4 and V5 production workers now retain immutable charged physical providers.
Full builds, startup checkpoint inspection, checkpoint reopen validation, and
private successors use admitted construction. A selected source accepts the
already charged shared allocation directly; successor preparation no longer
deep-clones a published provider through `Arc::make_mut`. Existing source proofs,
bindings, generations, authorization, and frontier checks remain in effect.

Recovery verifies the outer frame and checksum before allocating a temporary
program. A fixed 32-MiB allowance admits the existing byte-counted program decoder,
including malformed counts and canonical reproduction; compile-time guards bind
that maximum to the existing program widths, recursion, and input ceiling.
Borrowed row inspection then admits key and field storage, canonical outputs,
independent order indexes, position vectors, scalar copies, suffix keys, and
checkpoint reproduction before invoking the unchanged strict decoder.

Tests compare both checkpoint variants with ordinary recovery, preserve their
exact bytes, exercise pressure after sizing, reject checksum corruption and
truncated or trailing input, and reject checksum-valid invalid field identifiers
and excessive row counts. The final data capture retains the charge. Capacity
pressure follows the existing transient worker refusal; ordinary semantic
failures keep their previous integrity or per-generation capacity mapping.

Source assembly, catalog and policy scratch, delta mutation construction,
query results, protected cursors and retained epochs,
retirement, and derived artifact cleanup still require integration. All physical
families now have admitted build/recovery paths, but this does not establish the
complete aggregate budget or close WP-824.

Validation: all 171 selected projection/provider conformance and adjacent
recovery tests passed (471 excluded by selection); all ten adapter boundary tests
passed. Six scoped static acceptance checks passed, including formatting, clippy
with warnings denied, scope, handbook, file-size, and panic allowances.
The final production declaration check also passed all 118 selected tests
(524 excluded by selection).

PR note: Package WP-824 (open); Tier guarantee; Behavior: predicate recovery and
production ownership admission; Compatibility: unchanged V4/V5 bytes and query
semantics; Hazards/follow-ups: finish the remaining ownership and retirement
work before qualification. Existing handbook refusal behavior already covers
transient provider unavailability; this adds no application or operator control.

## Predicate construction and successor admission

Borrowed first-party admission now covers V4 and V5 predicate rebuilds and
private atomic successors. The physical predicate program owns a deep copy;
the separately shared compiled request keeps its existing registration charge.
Construction reserves before copying either the owned program or source rows.
The maximum includes sparse field trees, geometrically grown position vectors,
independent order arrays, string suffix keys, and canonical verification copies.
Successors additionally admit the private predecessor clone and temporary row
and duplicate-key trees before ordinary atomic apply.

The ordinary rebuild and apply implementations still validate bindings, field
profiles, row uniqueness, amplification limits, and frontier advancement.
Immutable settlement keeps the physical charge through the final data capture.
Tests compare both variants with ordinary construction and checkpoint bytes,
exercise pressure after sizing, verify successor isolation and typed semantic
refusal, and check final-reference release. Predicate recovery and production
ownership are described above; construction admission alone does not
claim aggregate enforcement, retirement, or package completion.

Validation: all 76 projection tests passed without skips. The final seven-step
scoped acceptance passed, including nextest, formatting, clippy with warnings
denied, handbook, scope, file-size, and panic-allowance checks.

PR note: Package WP-824 (open); Tier guarantee; Behavior: internal predicate
construction admission; Compatibility: existing logical semantics and durable
formats; Hazards/follow-ups: integrate recovery and production ownership before
qualification. No handbook operation changes because these are hidden
first-party construction helpers; existing public provider behavior is unchanged.

WP-824 remains open. The production slices establish one discovery lock
and monotonic access order across exact text, predicates, nullable predicates,
tokenized text, and long patterns. Primary and follower composition pass the
service cursor clock owner to the provider runtime. Failed/backward samples and
retention deadline overflow refuse admission without shortening obligations.

Each registration preserves its exact canonical key in a fixed 121-byte value.
The checkpoint header and file identity bytes remain compatible. Long-pattern
registrations retain a checked index into the immutable compiled program instead
of cloning the selected step; synthetic plan hashing preserves the original
32- or 64-byte input. Access ordinals rebase under the discovery lock before
overflow; rebasing preserves order and minimum retention deadlines.

Registration admission and explicit query/worker capture are wired into all
five production families. Physical build/checkpoint admission, cursor and retained
epoch ownership, retirement, fresh generation admission, and final-reference
artifact cleanup remain unfinished. Full 1-GiB ownership enforcement, sequential
churn, and sustained-workload qualification are not yet claimed. Existing slot
caps and exact freshness/authorization behavior remain in effect.

Validation: all 959 scoped server/service tests passed (eight existing skips),
including 22 focused provider tests. Formatting and clippy with warnings denied
also passed. The initial handbook attempt lacked tools on the worktree PATH;
a follow-up identified and corrected the new page's missing summary entry.
The handbook check then passed with the installed pinned tools. These checks include
provider key/checkpoint compatibility,
source capture and bounded delta application, primary/follower clock composition,
clock failures, deadline overflow, and monotonic access rebasing. Package closure
still requires all WP-824 deliverables and scoped/full acceptance.

PR note: Package WP-824 (open); Tier guarantee; Behavior: shared cursor/provider
retention clock and bounded registration access bookkeeping; Checks: recorded in
the implementing commit; Compatibility: existing canonical keys/checkpoint bytes,
query authorization and freshness, and slot caps; Hazards/follow-ups: complete
ownership admission and retirement before downstream qualification. Handbook:
`docs/configuration.md` records clock refusal behavior and current limits.

## Ownership foundation

The shared budget and owner primitives now have crate-run tests for reservation
admission, cancellation, overflow, shared final-reference release, allocation
metadata admission, and rejection of a charge from another budget. Deterministic
capture/retire/publication schedules prove that retirement closes new admission,
keeps captured bytes charged, and rejects obsolete publication. Owner creation,
query/worker pins, and publication guards are now production code. Retirement
transitions remain test-only until snapshot/cursor ownership and cleanup are
connected. The shared budget primitive lives in the types crate so its typed
charge can follow shared data destruction without duplicating accounting. These
pieces do not enforce the production aggregate budget yet.

## Retained capacity inspection

Trusted composition now has an exhaustive capacity inventory for compiled exact
text, predicate, nullable predicate, tokenized text, and long-pattern programs,
plus their retained policy and principal-fact data. Inspection counts vector and
string capacity, tree storage, nested owned values, and distinct shared
allocations. Process-local allocation identities remain opaque and redacted;
inspection cannot change canonical encodings or authorization values.

The walk refuses overflow, excessive recursion, excessive work, and more than
4,096 shared allocations. A failed walk cannot publish a partial report. The
caller must reserve the fixed 4-MiB inspection scratch before invoking it.
Exhaustive inventories and Copy-only inline fields make newly owning fields a
compile-time accounting review. This supports ownership admission; production
budget enforcement and retirement still require the unfinished integration above.

Capacity tests cover empty retained buffers, a retained tree root, repeated and
nested shared references, private program and audience buffer slack, bounded
recursion and shared census, ignored inspection errors, unchanged canonical
plan identity, and unchanged principal authority. Existing compatibility fixtures
continue to prove the compiled and durable formats. These helpers do not add an
application or operator control, so this slice changes no handbook operation.

Validation for this slice: all 668 scoped tests passed without skips. Scope,
formatting, clippy with warnings denied, generated artifacts, handbook,
downstream adapters, file size, and panic allowance checks passed. Initial
checks selected system Cargo and omitted the installed TypeScript compiler;
the final run used the installed pinned tools and required no fixture changes.

## Shared destruction ownership

Shared compiled-plan and policy payloads now carry a private destruction lifecycle
after their data fields. Shared strings, facts, and grant buffers keep a matching
sidecar through semantic clones. A numerical inspection capture also retains the
matching lifecycle after its original data reference, so inspection cannot cause
premature charge release. Deep value clones receive a fresh root lifecycle.

Attachment accepts only a typed first-party byte charge and retains one immutable
owner; admission must validate its allocation and budget identity. The control
implementation is closed, so an application cannot attach a user-defined Rust
destructor to database values. Dead lifecycles refuse attachment, and
weak attachment references cannot keep data alive or revive it. These fields do
not participate in canonical encoding, equality, order, hashing, or authority.
They prepare final-reference accounting; production provider admission, snapshot
pins, retirement, and cleanup remain unfinished. No aggregate enforcement is
claimed by this slice.

Validation: the full scoped acceptance plan passed, including all 1,036 tests
(eight existing skips), clippy with warnings denied, generated artifacts,
handbook, scope, file size, and panic allowance checks. Focused key regression
coverage also checks unchanged hashing, ordering, and debug/redaction behavior.
The initial prototype's arbitrary destruction attachment was replaced with a
typed first-party charge before committing; no arbitrary callback interface is
introduced.


## Registration admission

New registrations now inspect borrowed request fields before copying partition
values, constructing checkpoint paths, or allocating slots. All five families
use one process graph budget. A fixed allowance admits the bounded registry
metadata, with a compile-time check against the full tree-node layout bound.
Inspection scratch is reserved before traversal, and the completed allocation
census must provide a destruction lifecycle for every shared allocation.

Shared charges must match both the original allocation identity and this graph's
budget. A shared compilation is charged once across registrations; its charge
persists through the last compiled-data reference, including catalog references.
Concurrent attachment retains one immutable owner and releases the losing
reservation. Missing lifecycle evidence, foreign ownership, overflow, or budget
pressure refuses registration with the existing typed unavailable result.
Existing exact matches are touched without copying a new registration.

Reservation splitting transfers already admitted bytes without increasing the
aggregate admission. Cancelled and refused registration drops its unpublished
allowance; charges already attached to still-live compiled metadata remain until
that metadata is destroyed. This deliberately preserves the actual ownership
bound after partial admission.

This is registration accounting only. Provider rebuilds, checkpoint scratch,
captured snapshots, cursor pins, supersession, pressure retirement, and derived
cleanup still require integration before WP-824 can claim full aggregate budget
enforcement or the greater-than-256 registration churn exit gate.


Validation: all seven scoped acceptance steps passed, including 757 tests with
eight existing skips. Coverage includes deduplication through actual data
lifetime, foreign and undersized ownership refusal, pre-attachment saturation,
simultaneous admission, independent reservation splitting, and the existing
query, authorization, source-capture, archive, and replication recovery tests.

## Physical provider capacity

Exhaustive capacity inventories now cover the six physical provider states used
by the five registry families, including substring postings, token positions,
canonical outputs, sparse predicate fields, order arrays, and long-pattern gram
postings. Predicate scalar strings and byte buffers count retained capacity even
when their semantic value is empty. Inspection preserves checkpoint bytes and
provider identity; it does not replace the existing physical query algorithms.

A short checkpoint can rebuild much larger derived posting structures. Tests
therefore assert that decoded provider ownership includes those structures and
retained buffers, rather than accepting encoded length as a sufficient charge.
These inventories prepare provider preflight and settlement; they do not yet
reserve rebuild or decode allocations, or change any handbook operation.

Validation: all seven scoped acceptance steps passed, including 302 tests with
no skips, formatting, clippy with warnings denied, handbook, scope, file size,
and panic allowance checks. The initial test-declaration placement was corrected
before the passing run; no warning was suppressed.

## Candidate settlement

Wholly owned provider candidates can now settle an admitted maximum against a
completed capacity report. Inspection scratch is carved from the original work
allowance before traversal. Settlement only shrinks an allowance; incomplete
reports, shared payloads without graph admission, or an insufficient allowance
refuse publication.

The candidate guard destroys its data before releasing its reservation on every
refusal path. Successful settlement transfers ownership to an immutable data
allocation whose charge drops after its final data reference. In-place ledger
transitions keep the reservation intact if transfer fails. Tests observe the
budget during destruction, retained captures, and refusal; no timing sleep is
used. The actual rebuild/decode admission formulas and call sites remain open.

Validation: seven scoped acceptance checks passed, including 215 tests without
skips, formatting, clippy with warnings denied, handbook, scope, file size, and
panic allowance checks.

## Borrowed decode sizing

A bounded borrowed walk can now estimate canonical decoded heap capacity without
constructing strings, byte buffers, lists, records, or vectors. It reuses the
canonical decoder's primitive readers and tag constants, and the ownership
visitor's allocator allowance. Compact encoded lists therefore admit the much
larger decoded value slots before materialization. The complete report requires
one bounded document, canonical field ordering and scalar encodings, and no
trailing bytes; malformed or excessive input produces only a typed refusal.

The sizing result does not replace normal semantic decoding or account for the
containing root, transport and verification copies, or derived indexes. Those
allocations require their own conservative admission. This helper prepares
checkpoint and source-read preflight without changing canonical bytes, the normal
decoder, or any application or operator operation.

Validation: all seven scoped acceptance steps passed, including 222 tests without
skips, formatting, clippy with warnings denied, handbook, scope, file size, and
panic allowance checks. Differential properties compare borrowed sizing against
actual decoded capacity and decoder acceptance for generated and arbitrary inputs.


## Explicit query and worker capture

Every registered family now creates an owner from its admitted registration
charge. Query discovery and bounded worker discovery capture the actual slot and
an explicit charged pin while holding the shared registry lock. Dropping the
capture destroys its data reference before releasing the pin. Failed capture
returns the existing typed unavailable result; partially collected worker pins
release through ordinary destruction.

Worker selection updates acquire the owner publication guard before the slot
lock. The guard spans the complete selection update, preserving the lock order
registry, owner, slot and preventing a retirement check from racing publication.
Existing exact checkpoint validation and expected-selection comparisons remain.

The registry still does not retire or evict owners in this slice. Cursor capture,
retained epoch ownership, preflight for physical construction, generation naming,
and artifact reclamation remain required before enabling retirement. Tests keep
the retirement transitions available to prove capture/publication races.

Validation: the initial seven scoped acceptance steps passed, including 546 server
tests and eight existing skips. The final ownership follow-up passed six focused tests without skips and six
non-test acceptance checks. The pin allowance test failed before the guard and
charge allocations used the inventory allowance; it now passes. Owner admission
also precedes registration copies, preserving reservation lifetime on refusal.


## Encoded checkpoint read admission

Every provider recovery and checkpoint-reopen read now reserves a conservative
encoded buffer allowance before allocating or reading its payload. The length
comes from the already opened file and must fit the unchanged checkpoint ceiling.
The read fills one fixed-capacity buffer and refuses a changed length instead of
extending the allocation. Allocation or budget pressure remains a transient
rebuild refusal, allowing a later retry at the same application head.

The immutable buffer owns its charge through its final reference. Short, growing,
oversized, and failed reads release only their own admission. This covers encoded
read buffers; decoded providers, derived index reconstruction, checkpoint encoding
scratch, and source materialization remain separate unfinished admission work.

Validation: six non-test scoped acceptance checks passed. Thirty focused tests
passed, including all adapter tests, primary/follower provider conformance across
tail and restart, policy-dependent delta rebuilds, and checkpoint crash recovery.
The explicit filter excluded 529 unrelated or already-disabled tests; it did not
change the repository test configuration.


## Admitted encoded source reads

Storage helpers can now inspect the exact captured overlay before copying entity
points and partition/index byte rows. A borrowed point distinguishes checkpoint
fallback, replacement bytes, and tombstones. Forward overlay seeking uses
borrowed AVL nodes without allocating a payload or iterator stack. The merge
preserves the original physical ordering and suppresses deleted checkpoint rows.

The reader admits array and iterator metadata before allocation, then admits each
key/value copy while still borrowing its stored bytes. Immutable row captures
keep their charges independently of page discovery. Partial batch refusal drops
all copied rows before releasing array scratch, and iterator scratch is destroyed
before the array's ownership transfer. Prefix scratch is separately admitted.
Existing row and inspected-entry ceilings are unchanged.

These helpers do not decode or authorize records, switch snapshots, or grant
writer authority. Provider source materialization still uses its existing readers
until decoded-record, projection/policy scratch and retained source-row admission
are connected. Full aggregate enforcement and retirement remain unfinished.

Validation: all seven scoped non-test acceptance checks passed. Eighteen storage
API composite-view tests, seventeen existing storage-read conformance tests,
seven redb composite-view tests, and three focused provider-input tests passed.
The merge test also proves tombstones and partial-batch budget refusal release
all newly admitted rows while retaining the predecessor snapshot.

## Proposed durable decoder scope extension

Provider source decoding needs two changes in `crates/riffdb-proto/src/**`:
borrowed inspection of the already supported entity/index envelope payloads
before allocation, and a decode path whose canonical re-encoding scratch is
caller-admitted and released rather than retained in thread-local storage. The
source payload must still pass the normal readable schema, version, identity,
checksum, wire canonicality, generated-message canonicality and semantic checks.
Both existing V1 and V2 fixtures remain supported. No framing, schema identity,
record type, dependency or application interface changes are proposed.

Tests must compare the admitted path with the existing decoder on supported
fixtures and corrupt framing, and prove refusal before buffer construction and
release after failures. The human approved this scope extension on 2026-10-02; a standalone
package-scope commit records it before Protobuf crate source changes. Storage-reader integration and physical provider
construction sizing can proceed within the already approved scope.

## Exact-text V2 rebuild admission

A hidden first-party rebuild entry point now reserves a prospective construction
maximum before invoking the existing V2 builder. It counts both retained posting
families, all byte-boundary substring occurrences without relying on deduplication,
term bytes, allocator slack per buffer, posting growth, ordering arrays and their
cloned text/key buffers. It also covers deep output copies, temporary term sets,
hashed source trees, encoding scratch and the final inventory. Prospective trees
use the same node allowance as retained-tree inspection.

Borrowed canonical length checks reject oversized outputs before encoding. The
ordinary builder and checkpoint representation are unchanged. Successful builds
settle into an immutable allocation whose charge lasts through its final capture;
admission pressure remains distinct from ordinary provider validation errors.
Tests compare ordinary and admitted states on empty, repeated, distinct and UTF-8
text and multiple posting populations, and check charge release and early refusal.

This entry point is not yet connected to server publication: checkpoint recovery,
incremental successor admission, source materialization and the remaining provider
families must support the same ownership before changing retained state types.

Validation: all seven scoped acceptance steps passed, including 280 tests without
skips, Clippy with warnings denied, formatting, scope, handbook and size checks.
The initial rebuild-admission test failed because the entry point did not exist;
the implemented path passes its state, checkpoint and lifetime assertions.

## Exact-text V2 decoded checkpoint admission

Checkpoint recovery now has a hidden admitted entry point. Its borrowed walk
checks existing framing and row ceilings, sizes canonical output without decoding,
and reserves row copies, derived postings, ordering scratch, final inventory and
canonical re-encoding before calling the unchanged recovery decoder. The encoding
allowance covers both the initial row-capacity hint and later buffer growth.
Borrowed sizing does not authenticate a provider; the ordinary decoder still
checks keys, hash ordering, duplicates, payload semantics and exact checkpoint
byte identity before a candidate can be returned.

Successful recovery retains an immutable charged allocation through its final
capture. Invalid framing fails closed; budget pressure after borrowed inspection
releases inspection scratch without publishing decoded ownership. Server recovery
and retained-state integration remain pending the other construction paths.

Validation: six scoped acceptance steps passed, including all 61 projection
tests without skips. Recovery tests cover empty and UTF-8 text, multiple posting
populations, truncated/trailing bytes, row-capacity hints, final captures and
pressure after borrowed inspection.


## Admitted provider entity decoding and pages

A closed first-party decoder now inspects the existing entity and index framing
without allocating generated messages. It shares the ordinary decoder's readable
schema, identity, checksum and wire preflight. Canonical re-encoding uses
caller-admitted scratch rather than retaining a thread-local buffer. Both V1 and
V2 remain supported; semantic reconstruction still uses the ordinary entity
mapper and its canonical checks.

Encoded input, generated messages, semantic reconstruction and page metadata
have separate admissions. Shared canonical fields retain their actual charged
original record through semantic record clones, so dropping a page cannot release
live field ownership. Point reads verify the physical target. Partition reads
use the original page validator, ordering, continuation and scan ceilings. They
require the captured root's frontier to have been validated by ordinary source
capture, and refuse an uninitialized cache before reading a page.

Tests compare ordinary and admitted messages across both framing variants and
byte mutations, verify explicit scratch release, and exercise semantic rejection,
physical-key substitution and refusal partway through a decoded page. Exact-text and predicate full rebuilds now use the admitted entity point reader
on their existing captured snapshot. A production source test proves shared-budget
refusal and capture stability across a later authoritative commit. Partition-page
integration is recorded in the next slice below. Projection/policy work, physical
provider successors and retirement still prevent claiming aggregate enforcement
or package closure.


## Filtered exact-text construction and recovery admission

V3 now has hidden admitted rebuild and checkpoint recovery entry points. Their
prospective maxima include the base posting families, disjoint filtered posting
families, membership maps, canonical filter keys, row/key copies and ordering
scratch. Recovery inspects the supported wrapper and embedded V2 framing before
allocation, then delegates all semantic, membership and canonicality checks to
the unchanged decoder. It preserves the existing recovery filter ceiling rather
than applying the narrower rebuild-only ceiling to stored data.

Tests compare ordinary/admitted state and checkpoint bytes, check the complete
retained capacity inventory across empty, repeated and Unicode text and several
filter types, and prove refusal after inspection without provider construction.
Recovery also covers truncated/trailing framing, wider compatible filters and
final-capture ownership. Server publication and incremental successors remain
unconnected to these construction entry points.

Validation: all nine storage acceptance steps passed, including 1,869 tests
with one existing skip. All 552 server tests passed with eight existing skips;
all 66 projection tests passed without skips. All eight final non-test acceptance
steps passed for the five affected crates, including formatting, Clippy with
warnings denied, generated artifacts, scope, topology and handbook checks.
Earlier checks detected concurrent edits and two test-only unnecessary borrows;
both were corrected and the affected checks rerun successfully.

PR note: Package WP-824 (open); Tier guarantee; Behavior: admitted provider source
decoding and point/page captures, production entity point admission, and filtered
text construction/recovery admission. Compatibility: existing wire/checkpoint
formats, query ordering, authorization and scan ceilings remain unchanged.
Handbook: this implementation page is reachable from SUMMARY.md. Hazards and
follow-ups: complete source/policy/successor accounting, protected epochs, safe
retirement and artifact cleanup before qualification and package closure.


## Admitted provider index pages and worker integration

The closed admitted decoder now also supports the existing index-generation
metadata used by source pages. Both supported envelope versions still run the
ordinary semantic mapper. Index records attach their charge to the actual
encoded buffer, keeping shared bytes charged through the last record clone.
Deep covered values are independently admitted before copying into a page.

A reservation can absorb another reservation from the same graph before a new
row allocation. The transfer preserves total reserved bytes and reduces the
live reservation count; foreign-graph or invalid transfers refuse. Deterministic
barrier tests cover transfer across threads, saturation and final-reference
release without introducing another synchronization primitive.

Index pages use the same captured source, physical-key checks, partition filter,
index epoch, continuation validation and existing inspection and byte ceilings.
Tests compare ordinary and admitted pages, reject substituted keys even in
another partition, and prove cleanup when pressure refuses a partially built
page. Exact-text and predicate workers now use admitted index pages, and the
tokenized-text rebuild uses admitted entity partition pages. Public execute paths
retain their existing prohibition on authoritative scans and hydration.

Validation so far: all 981 types, Protobuf and storage API tests passed without
skips; all 27 adapter tests passed. All eight scoped non-test acceptance checks passed. The broader
1,670-test storage/server conformance run finished with 1,669 passes,
one timeout and nine existing skips. The timed-out bootstrap publication
crash test passed in isolation in 25.6 seconds under the same 120-second
limit. The original failure log is preserved; a clean full-package run remains
required before closure. Remaining
provider construction, projection/policy work, protected epoch leases, safe
retirement and artifact cleanup remain incomplete; WP-824 stays open and aggregate enforcement is not claimed.

PR note: Package WP-824 (open); Tier guarantee; Behavior: admitted index records,
index-generation metadata and source pages, with worker page integration.
Compatibility: existing envelopes, source snapshots, ordering, authorization,
continuations and scan ceilings. Handbook: this implementation page records the
internal progress; no application method or operator control changes. Hazards
and follow-ups: complete construction and successor ownership, protected epoch
leases, retirement and cleanup before qualification.


## Production binary-text provider ownership

V2 and V3 successor admission reserves conservative maxima before cloning the
predecessor or invoking ordinary atomic apply. It counts simultaneous private
clones, mutation copies, prospective posting and ordering growth, filter keys,
filter families and canonical checkpoint validation. Original state and borrowed
mutations remain independently owned. Settlement releases excess reservation
and keeps the actual immutable provider charged through its final capture.

Production binary-text rebuild, checkpoint recovery/reopen and incremental
catch-up now use the admitted entry points. Retained provider variants hold the
charged allocations directly; semantic equality ignores accounting metadata.
Budget pressure returns the existing transient rebuild refusal, while ordinary
validation and generation failures preserve their existing typed behavior.
The hidden successor entry points currently admit binary UTF-8, the profile used
by these production workers; other normalization profiles refuse this path until
their construction amplification is proved. Ordinary provider APIs are unchanged.

Tests compare ordinary apply and checkpoint bytes across insertions, replacements,
Unicode/repeated text, filter moves and deletions. They check unchanged predecessor
captures, pressure after inspection, invalid mutations, failed epoch advancement
and final-reference release. All three focused successor tests and all 27 adapter
tests passed. All 100 selected projection/provider conformance tests passed;
534 unrelated tests were excluded by the selection expression. All six scoped
non-test acceptance checks passed, including Clippy with warnings denied,
formatting, scope, handbook, file size and panic allowance checks. Initial test
fixture and compile errors were corrected before these passing runs.

WP-824 remains open. Predicate, tokenized and long-pattern construction,
source/catalog/policy scratch, checkpoint encoding, protected epochs, retirement
and artifact cleanup still prevent aggregate enforcement and milestone closure.

PR note: Package WP-824 (open); Tier guarantee; Behavior: production binary-text
provider construction, recovery and successor ownership admission. Compatibility:
unchanged provider algorithms, checkpoint bytes, generation/frontier validation,
authorization and application interfaces. Handbook: this implementation page.
Hazards and follow-ups: complete the remaining ownership and retirement paths
before qualification; this slice does not claim the 24-hour rehearsal.


## Production tokenized provider ownership

Tokenized rebuild admission now reserves configuration copies, analyzed terms,
positions, posting trees, normalization workspace and checkpoint-validation
buffers before invoking the existing builder. Keyword and standard analyzers
retain their frozen meanings. The existing exhaustive Unicode scalar test also
checks NFKD and NFKC intermediate expansion against the frozen bound used for
normalization workspace admission; no normalization or tokenization changes.

Recovery walks borrowed existing checkpoint lengths before allocating documents,
canonical outputs or redundant postings. The ordinary decoder still validates
field identities, positions, hash/key identity, duplicate entries and agreement
with rebuilt postings. Retained tokenized epochs now hold immutable charged
allocations, preserving ownership through the final epoch reference. Pressure
uses the existing transient rebuild refusal; ordinary provider limits retain their
existing typed capacity behavior. Retention counts and epoch selection are unchanged.

Focused tests compare both analyzers and checkpoint bytes on empty, repeated and
Unicode text, multiple documents, partial admission pressure and invalid
populations. Recovery tests cover truncated/trailing data, invalid fields, changed
redundant postings and pressure after inspection. Both focused tests, the expanded
Unicode scalar proof and all 27 adapter tests pass. All 324 selected types, projection and provider conformance tests passed;
534 unrelated tests were excluded by the selection expression. All six scoped
non-test acceptance checks passed, including formatting, Clippy with warnings
denied, scope, handbook, file size and panic allowances.

WP-824 remains open. Predicate and long-pattern construction, source/catalog/policy
scratch, checkpoint encoding, protected cursor leases, retirement and final-reference
artifact cleanup remain unfinished. No aggregate-enforcement or rehearsal claim.

PR note: Package WP-824 (open); Tier guarantee; Behavior: production tokenized
construction/recovery and retained epoch ownership admission. Compatibility:
unchanged analyzers, V1 checkpoint format, semantic decoder, authorization,
epoch selection and application interfaces. Handbook: this implementation page.
Hazards and follow-ups: finish the remaining ownership and retirement lifecycle
before qualification.


## Posting-tree root admission follow-up

A long-pattern recovery test identified an omitted allowance for separate
posting-set roots. The same review added an independent peak-inventory assertion
to tokenized recovery, including a 64-term distinct vocabulary; that assertion
failed before the bound was corrected. Tokenized construction and recovery now
count each possible posting-document tree root alongside member nodes and
per-root allocation metadata. Ordinary algorithms and checkpoint bytes are
unchanged. All six scoped non-test acceptance checks passed, and all 102 selected
projection/provider conformance tests passed; 534 unrelated tests were excluded
by selection.

The combined source-page/text/tokenized integration passed all eight non-test
acceptance checks and all 1,085 selected types, Protobuf, storage API, projection
and provider conformance tests; 534 unrelated tests were excluded by selection.
WP-824 remains open with the ownership, epoch protection and retirement follow-ups
listed above. This receipt does not claim a clean full CI or a real rehearsal.


## Production long-pattern provider ownership

A private admitted builder now reserves row transformation, gram vectors,
release copies, posting member nodes and independent posting-set roots before
ordinary checked upsert. Partial candidates keep every earlier row's reservation;
failure and cancellation destroy data before releasing work. Finishing settles
one immutable charged allocation. Recovery inspects borrowed lengths and impossible
gram declarations before ordinary digest/gram validation and posting reconstruction.

Production long-pattern rebuild and checkpoint recovery now retain those charged
allocations in their epoch state. Rebuild reads one captured immutable source root
at the expected head, uses admitted entity pages, and reserves input metadata and
release-encoding scratch before copying. Existing policy admission, generation
checks, profile transformations, checkpoint bytes and epoch selection remain.
Broader catalog/policy/source ownership and protected cursor leases remain open.

Three focused tests cover both profiles, empty/Unicode/repeated text, replacement,
final captures, partial pressure, cancellation, row limits, corrupt/truncated/trailing
checkpoints and hostile gram counts. An initial recovery bound omitted separate
posting-set roots; the regression identified the shortage and the corrected bound
passes. Production checks initially detected an Option/frontier type mismatch and
an outdated structural count; both were corrected. All six scoped non-test acceptance checks passed, all 27 adapter tests passed,
and all 105 selected projection/provider conformance tests passed. The selection
excluded 534 unrelated tests; the earlier failures remain in their original logs.

PR note: Package WP-824 (open); Tier guarantee; Behavior: admitted long-pattern
construction/recovery, retained physical ownership and captured budgeted input pages.
Compatibility: existing profiles, bounds, ordinary validation, durable format,
authorization, generation/frontier checks, epoch selection and application interfaces.
Handbook: this implementation page. Hazards and follow-ups: finish predicate and
remaining source/policy/checkpoint ownership, protect retained cursors, retire eligible
owners and clean derived artifacts before qualification. No full aggregate budget,
package closure or rehearsal is claimed.
