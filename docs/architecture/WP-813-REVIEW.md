# WP-813: Per-write proof review

ADR-0262 was accepted on 2026-09-28, with delegated ADR acceptance during
implementation and human review reserved before merge. Work is on a dedicated
worktree. Main has not been merged; remove the worktree after the reviewed merge.
Package verification is in progress; this note does not declare closure.

## Behavior

A revision increment no longer authorizes replacement writes to other fields.
Each assignment carries all applicable evidence: initialization, narrow
same-field arithmetic, an explicit `checked_by` business association, an exact
same-root caller-version fence, or deliberate overwrite. Invalid associations
fail even when another proof independently admits the write.

Business relevance remains an author claim. A vacuous stored-state predicate
can satisfy the structural association checks. Arithmetic does not establish
freshness of a supplied delta. A version fence depends on relevant writers
advancing the same revision and callers supplying the revision they observed.
These limitations are part of the accepted decision, not inferred guarantees.

V25 grammar, executable IR, and bundle framing include canonical, bounded proof
metadata in command identity. Bundle construction and decoding rederive and
validate it. New compilation of assignment-bearing commands uses V25; existing
artifacts retain their bytes, hashes, validation, and execution. Execution order,
read validation, authorization, and commit sequencing are unchanged.

## Source migration review

- Budget timestamps associate with the existing sufficient-budget requirement;
  allocation remains an arithmetic delta and creation remains initialization.
- Inventory reservation timestamps associate with sufficient stock; order status
  and timestamps associate with the existing open-order check.
- Bulk indexed mutations associate replacements with the existing per-element
  expected-revision check. This is named business evidence; it is not relabeled
  as the narrower unconditional aggregate-root fence.
- Token consumption associates with the unused-token check. Session digest
  refresh associates with expiry; the existing workflow transition still checks
  its typed revision. Run completion associates with the running-state check.
- Deliberate unconditional initialized-state payload writes, decision-arm literal
  replacements, secret-copy fixtures, and index replacement test commands use
  overwrite. Their existing runtime predicates and secret disclosures remain.
- Checkout reservation release adds another stored row's quantity to stock.
  This falls outside the accepted state-independent-delta recognizer, so it uses
  explicit overwrite while retaining the exact reservation delete and order check.
  No arbitrary product predicate is invented to obtain a business-proof label.
- Current clients, bundle fixtures, application locks, protocol schema-catalog
  identity, and explain response-charge fixtures are regenerated through their
  owning renderers. Frozen predecessor bundles keep compatibility tests distinct
  from current source recompilation.

## Verification

The initial compiler regression failed under the previous rule. The archived
initialized-state runtime characterization still demonstrates two serialized
successful replacements despite revision increments. Current compiler tests
cover narrow arithmetic, explicit association requirements, invalid suffixes,
creation, decision and bulk scopes, explain output, hashes, and legacy decoding.

Deterministic coordinator coverage verifies stale replacement refusal, exclusive
claims, accumulated deltas, deliberate overwrite, replay, and persisted outcome,
commit, entity version, and provenance after reopen. Application-service coverage
verifies associated budget checks and arithmetic through current authorization,
including refusal and replay retaining outcome, sequence, and provenance.
Proof-codec tests cover round trips, malformed tags and bounds, fabricated proof
kinds, missing/duplicate metadata, and incorrect assignment locations.

## PR note

- Package: WP-813; Tier: guarantee.
- Behavior: strict per-assignment evidence replaces binding-wide dependency
  authorization; explicit named business associations and V25 proof metadata.
- Checks: focused compiler, runtime, coordinator, and application-service tests
  pass. Package acceptance and the full merge battery remain in progress.
- Compatibility: source recompilation and new plan identities intentionally
  change. Historical artifacts keep their behavior and are not silently upgraded.
- Handbook: language and authoring references, command/invariant cookbook, compatibility,
  safety-by-construction, and known limitations describe the syntax, proofs, execution-scope limits, and
  migration boundary. Generated format and diagnostic references are refreshed.
- Hazards and follow-ups: semantic predicate relevance, stale input deltas, and
  revision protocol consistency remain application responsibilities. Conditional
  fences and broader algebraic inference remain deferred. Human review precedes
  merge to main and worktree cleanup.
