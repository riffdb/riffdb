# ADR-0068: Database History Incarnation

- **Status:** Proposed
- **Date:** 2026-07-30
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `REC-002`, `R-16` (spec risk register), `STO-001`
- **Related work packages:** Package F (pre-alpha hardening)
- **Amends:** ADR-0019 (retained metadata set), ADR-0050 (restore limitation)

## Context

Destructive restore preserves `DatabaseId` and deterministically rewinds both
sequence allocators to the backup frontier, so application and administration
sequences from the destroyed suffix are structurally guaranteed to be reused.
No durable value distinguishes a restore from a restart: `ServerGenerationV1`
is random and per-process. Every locator, cursor, subscription position,
read-after-sequence expectation, and idempotency assumption from a destroyed
suffix silently binds to unrelated data. ADR-0050 documented this and required
new human review for any incarnation mechanism; the spec risk register rates
it Medium likelihood, Critical impact. After alpha, adding a fence becomes a
breaking migration against live client assumptions; before alpha it is an
additive change.

## Decision

### One durable, monotonic history incarnation

- A seventh retained-metadata key, `history_incarnation/v1`, stores
  `StoredHistoryIncarnationV1 { incarnation: u64 }`. Bootstrap value is 1.
- Existing databases gain the key via the record-registry migration mechanism
  (a pre-migration digest pins the current registry; one immediate write
  transaction inserts the key; the registry is republished).
- The incarnation changes in exactly one place: destructive restore. During
  the offline phase the driver computes
  `new = max(target incarnation, staged incarnation) + 1` and records it in
  the maintenance receipt; during the published phase it idempotently stamps
  the value into the restored database before post-publication validation.
  Crash-resume re-applies the stamp from the receipt, exactly once in effect.
  Ordinary startup, non-destructive maintenance, and already-stamped resumes
  never change it.

### Backups carry the incarnation

The offline backup manifest gains an optional `history_incarnation`. A
manifest without the field (pre-fence backup) restores normally and is treated
as incarnation 0 for the bump computation — the fence still advances.
The manifest version is unchanged.

### Public exposure and validation

- Responses that carry commit-sequence-derived values expose the current
  `history_incarnation` as an additive field (command outcomes, commit pages
  and notifications, discovery fences beside the process generation, health
  and stats).
- Sequence-anchored requests (commit subscription `after`, commit point reads,
  commit scans) accept an optional `observed_history_incarnation`. Absent
  means unvalidated — pre-fence clients keep working. Present and different
  from the current value is rejected with the typed
  `history_incarnation_mismatch` error (`RDB-HISTORY-0101`,
  failed-precondition class, correct-request recovery): the caller's
  observations predate a restore and must be rebuilt from fresh reads.
- Idempotency identity, per-record formats, provenance locator bytes, and
  cursor token formats are unchanged. The fence makes staleness detectable at
  the APIs that resolve observations; it does not stamp history into
  identities.

## Consequences

- Sequence reuse after restore remains possible, but clients that carry the
  incarnation can no longer silently bind stale observations to new data.
- The retained-metadata inventory grows to seven; startup validation,
  structural evidence, backup manifests, and the maintenance receipt all learn
  the new value.
- Pre-fence backups stay restorable forever; pre-fence clients keep working
  unvalidated until they adopt the field.
- ADR-0050's known limitation narrows from "undetectable" to "detectable by
  participating clients"; the residual risk (non-participating clients) stays
  documented.

## Rejected alternatives

- **UUID incarnation.** Loses ordering: clients could not distinguish stale
  from merely different, and startup could not assert manifest ≤ current.
- **Incarnation inside idempotency identity or locators.** A frozen-format
  break with per-record cost, rejected for the POC-to-alpha window; the
  detection contract achieves the safety property additively.
- **Reusing `ServerGenerationV1`.** Random and non-durable; cannot distinguish
  restore from restart. Both values remain exposed for their distinct
  purposes.
- **Refusing pre-fence backups.** Breaks every existing backup for no safety
  gain over accept-and-stamp.

## Acceptance

Pending maintainer acceptance. Package F merges only after this record is
accepted.
