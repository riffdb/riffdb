---
adr: "0262"
title: Separate Write Dependencies From Precondition Proofs
status: accepted
tier: guarantee
date: 2026-09-28
accepted: 2026-09-28
acceptance: 'maintainer, in session, 2026-09-28: i accept; delegated acceptance of
  further ADRs for implementing this plan, with human review before merge to main'
requires: [ADR-0256]
amends: [ADR-0256]
supersedes: []
requirements: [CMP-015, CMP-013]
packages: [WP-813]
obligations: []
review_triggers:
  - Widening the recognized arithmetic or revision-fence shapes.
  - Treating an author-associated business check as proof of caller intent.
  - Reinterpreting historical plans or changing transaction execution semantics.
  - Changing proof encoding, canonical identity, or association scope.
---
# ADR-0262: Separate Write Dependencies From Precondition Proofs

## Context

ADR-0256 and CMP-015 let any stored-field read authorize all writes on that
binding. Incrementing revision therefore admits a replacement payload without
checking the caller's version. Two serialized executions can both succeed and
replace each other's payloads. Transaction-current read validation does not
establish freshness of caller intent. This is a safety defect investigation
under governance directive D-001, not an unrelated feature addition.

The compiler implements the accepted rule; correcting it requires changing the
rule. The existing root compare-and-increment fence is useful evidence, but its
local-read alternative admits the weaker pattern. This record replaces that
alternative and the claim that every accepted set prevents a silent lost edit.
The record was accepted on 2026-09-28; public behavior changes when its
implementation lands. The maintainer also delegated further ADR acceptance for
this implementation and reserved review of all changes before merge to main.

## Decision

1. Classify each assignment occurrence, including each decision-arm occurrence,
   by its evidence: initialization, state-derived update, named business check,
   caller-version fence, or explicit overwrite. Preserve every applicable proof
   rather than collapsing evidence into a binding-wide guarded flag. No kind
   implies semantic relevance of arbitrary expressions to application intent.
2. For a binding with a pre-image, infer a state-derived update only from
   `target + delta`, `delta + target`, or `target - delta`, assigned to that
   exact stored target field. Delta may use validated input, key values, and
   constants, but no stored-state dependency. Existing numeric type checking
   and checked arithmetic apply. Parentheses do not change matching. Aliases,
   cancellation, multiplication, and other algebraic equivalences are not
   inferred. This proves the stated transformation, not freshness of the delta.
3. Add the optional source suffix `checked_by requirement_name` to `set`, after
   its value expression and before an existing `reveals` clause. For example:

   ```riff
   require is_free: slot.free else SlotTaken {}
   set slot.free = false checked_by is_free
   set slot.held_by = holder checked_by is_free
   ```

   Resolve one requirement name in the write's lexical command/iteration
   scope, including visible outer requirements. Reject unresolved, ambiguous,
   non-dominating, and out-of-scope names. The requirement must read a stored
   field on the assignment's binding. A same-record key read, unrelated binding
   read, or input-only predicate is insufficient. Preserve normal source-order
   and binding-visibility checks. An invalid suffix rejects even when another
   proof would independently admit the write. Disallow this suffix on overwrite
   and on creation; initialization-capable bindings use the existing-row rule.
4. A business association proves that the named requirement executes before
   the write on every path reaching it and reads the stated binding. The author
   explicitly asserts its relevance; the compiler does not prove relevance,
   predicate strength, or stale-intent protection. A vacuous stored-state
   predicate can satisfy these structural checks and MUST NOT be represented
   as a caller-version fence. Documentation and explain output state this limit.
5. Preserve ADR-0256 Decision 6's exact unconditional compare-and-increment
   fence, including signedness, checked overflow, ordinary mutate root,
   aggregate membership, and exact root-key fingerprints. Apply it to both
   root and matching child writes. The fence tests the supplied expected value;
   freshness requires relevant writers to advance the same revision and callers
   to supply the revision they observed. This is not schema-wide enforcement
   that every command maintains that protocol. Conditional fences remain out.
6. A pre-image `set` requires evidence from Decisions 2, 3, or 5. No unrelated
   assignment, unassociated requirement, decision predicate, sibling branch,
   invariant, or mere stored-field read supplies that evidence. An overwrite
   is permitted only without such evidence; initialization uses set. Retain
   RDB-C048 and RDB-C049 for spelling mismatches with corrected guidance, and
   add typed association diagnostics with source-span snapshots. Deletes keep
   their existing behavior; this decision concerns field assignments.
7. Carry typed proof metadata in the executable IR per assignment occurrence,
   including decision location and referenced requirement or fence evidence.
   Canonically order it and include it in plan identity. Introduce the next
   available grammar, IR, and bundle versions, allocated together according to
   existing version-topology policy. New compilation of commands with field
   assignments uses the new proof-bearing format. The metadata validator checks
   completeness and consistency against expressions and control flow before
   admitting a new-format plan; malformed or contradictory evidence fails closed.
   No execution, commit ordering, read validation, or mutation semantics change.
8. Extend typed and rendered contract explain output with proof kinds and
   evidence locations. Preserve the existing unguarded-writes accessor for
   compatibility, but do not present an empty set there as stale-intent proof.
   Historical artifacts retain their bytes, hashes, validation, and execution;
   describe their classification as legacy dependency evidence, never inferred
   new proofs. New proof encodings have round-trip and refusal fixtures.
9. Enforce strictly on recompilation, without a warning-only or opt-out mode.
   Migrate each affected source by identifying its actual intended policy:
   arithmetic transformation, explicitly associated business requirement,
   existing revision fence, or deliberate overwrite. No automatic addition of
   arbitrary requirements or blind mechanical respelling is allowed. Rebuild
   generated references and fixtures and renew application locks normally.
   Preserve historical compatibility evidence using pinned old artifacts.
10. On implementation, replace CMP-015's binding-wide definition and its claim
    about revision increments with these rules; update CMP-013's explanation
    surface and the reachable handbook. The cookbook must demonstrate both
    payload replacement with a caller revision and an associated business
    transition, and distinguish both from a counter increment.

## Options considered

- Rename the existing classification only: rejected; unrelated reads would
  still authorize replacement set writes without any explicit association.
- Require caller versions for all updates: rejected; counters and application
  business transitions have useful, different semantics.
- Accept every expression reading the target: rejected; `old * 0 + replacement`
  would authorize an effective replacement under a state-derived label.
- Infer business relevance from arbitrary predicates: rejected; this POC uses
  bounded structural analysis and explicit author claims, not theorem proving.

## Consequences

- Recompilation is intentionally breaking and may move all assignment-bearing
  command hashes even when runtime behavior is unchanged. Existing deployments
  require migration to gain the stronger source checks.
- Existing decision-driven replacements need a dominating named requirement,
  an unconditional root fence, or an intentional overwrite. Decision predicates
  remain runtime conditions but are not implicitly named business associations.
- Child-local fences, arbitrary equivalent arithmetic, named decision-arm
  predicates, and schema-wide revision protocol enforcement are deferred.

## Standing design tests

- **Interface safety:** unrelated reads cannot silently authorize replacement
  set writes. A business association is an explicit author claim, not a proof
  of stale-intent protection. Neither it nor arithmetic receives that label.
  Authors can still declare a semantically weak requirement, a stale delta, or
  an inconsistent revision protocol. Exact human acceptance must explicitly
  accept these limits in place of ADR-0256's universal no-silent-lost-update
  claim; transactions, idempotency, scoping, and durability remain mandatory.
- **Scale:** operate over existing bounded expressions, effects, requirements,
  and scopes. Charge added syntax and metadata to existing collection, input,
  output, recursion, and diagnostic budgets. No data scan or runtime I/O is
  added, and proof analysis does not depend on population size.

## Checks

- Before implementation, the runtime characterization
  `revision_increment_alone_allows_serialized_payload_replacement` demonstrates
  two transaction-current evaluations producing replacement candidates with
  increasing revision; it does not claim coordinator or durability coverage.
- Compiler regressions cover arithmetic boundaries, association name/scope and
  dominance, removal of requirements, initialization, cross-arm contamination,
  and all existing fence rejection cases, with semantic and span assertions.
- Deterministic service/coordinator tests cover stale fenced refusal, exclusive
  slot claims, accumulated deltas, deliberate overwrites, provenance, and
  idempotent replay. No correctness test synchronizes through sleeps.
- Hash, canonical encoding, malformed-metadata, historical-artifact, explain,
  downstream-adapter, handbook, package acceptance, and full merge checks pass.
