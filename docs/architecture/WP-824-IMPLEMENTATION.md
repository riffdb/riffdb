# WP-824 implementation progress

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
