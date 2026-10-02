# WP-824 implementation progress

WP-824 remains open. This first production slice establishes one discovery lock
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
The budget/owner prototypes are not production registration accounting. No churn,
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
keeps captured bytes charged, and rejects obsolete publication. These modules
remain `cfg(test)` until production preflight, snapshot/cursor pins, and cleanup
are connected. They do not enforce the production aggregate budget yet.

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
