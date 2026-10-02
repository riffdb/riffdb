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

The aggregate allocation budget, query/worker/cursor capture pins, retirement,
fresh generation admission, and final-reference artifact cleanup are unfinished.
Registration admission is wired; the owner/pin prototype still needs integration. No churn,
1-GiB ownership bound, or sustained-workload qualification is claimed by this
slice. Existing caps and exact freshness/authorization behavior remain in effect.

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

## Ownership foundation (test-only)

The shared budget and owner primitives now have crate-run tests for reservation
admission, cancellation, overflow, shared final-reference release, allocation
metadata admission, and rejection of a charge from another budget. Deterministic
capture/retire/publication schedules prove that retirement closes new admission,
keeps captured bytes charged, and rejects obsolete publication. The server owner prototype
remains `cfg(test)` until production preflight, snapshot/cursor pins, and cleanup
are connected. The shared budget primitive lives in the types crate so its typed
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
