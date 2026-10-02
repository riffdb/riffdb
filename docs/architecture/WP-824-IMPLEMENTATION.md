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
