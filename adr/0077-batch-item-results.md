# ADR-0077: Batch Item Results

- **Status:** Proposed
- **Date:** 2026-07-31
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `ID-004`, `REC-001`
- **Related work packages:** Package B (format acceptances)
- **Amends:** ADR-0059 (bounded command batching), ADR-0071 (extends the
  certain-not-executed scope to batch items)

## Context

The public batch response carries only success rows. One item's typed
rejection therefore collapses the entire RPC into a single error for raw
callers, discarding sibling results that may already be durable; only the
Rust SDK recovers, by re-entering every item through idempotent same-key
replay. This whole-RPC-failure behavior hardens into a de-facto contract the
day alpha clients exist, after which adding per-item carriage changes
observable behavior raw callers depend on. Batch semantics themselves
(ADR-0059: at most 16 independent, non-atomic, input-ordered items) are
unchanged by this record.

## Decision

### Per-item result carriage

The batch response gains an always-populated, input-ordered item list whose
length equals the request's, where each item is exactly one of: the item's
ordinary command response, or the item's typed application error carrying the
batch operation identity. The typed error is captured from the service result
directly, never reconstructed from transport status.

### Legacy field rule

The legacy success-row field is populated if and only if every item
succeeded, in which case it mirrors the item list positionally. On any item
error it is empty — a validating old reader rejects the empty list and falls
back to per-item recovery, and a non-validating positional consumer reads
zero rows; positional misalignment is unrepresentable in either case.

### Failure-class boundary

Service-control failures without an application identity (cancellation,
deadline, response-size, emergency containment) still fail the whole RPC;
fabricating per-item identities for them would weaken fail-closed
containment. All application-classified failures — including typed capacity
rejection — are carried per item, extending certain-not-executed semantics to
individual batch items for every caller.

### Client guidance

SDKs consume the item list when present; items whose error the registry
classifies as retryable — recovery action retry or
resolve-with-same-idempotency-key — re-enter bounded same-key recovery
(preserving the pre-item-carriage retry budget), and non-retryable errors
surface directly with no re-entry. Absent the item list (older server),
existing behavior is unchanged.

## Consequences

- Raw batch callers get per-item certainty without SDK mediation.
- One slow or rejected item no longer discards committed siblings' results.
- Public response schema grows additively; frozen surface hashes rotate once,
  now, while no external clients exist.
- Legacy mirroring doubles the encoded payload of an all-success batch,
  halving the effective aggregate response capacity under the public response
  bound — an accepted cost of old-reader safety, removable with the legacy
  field after alpha clients migrate.
- ADR-0059's batching section gains the item-carriage and legacy-field rules.

## Rejected alternatives

- **Parallel error array with indices.** Two correlated arrays are a
  standing foot-gun for positional bugs.
- **Permanent single-command scoping.** Freezes the whole-RPC collapse
  forever and keeps raw callers dependent on client-side recovery for
  results that already exist.
- **Partial legacy-field population.** Silently misaligns old readers'
  positional consumption — the worst failure mode available here.

## Acceptance

Pending maintainer acceptance. Package B merges only after this record is
accepted.
