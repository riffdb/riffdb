# ADR-0085: History Retention and Startup Scaling

- **Status:** Accepted
- **Date:** 2026-08-01
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `REC-001`, `PERF-013`
- **Amends:** ADR-0019 (validated-prefix checkpoint), ADR-0073 (startup-scale evidence), ADR-0082 (retention conclusions)

## Context

Measured on real disk (startup-scale harness, 2026-08-01): a database of 65,536
retained commands opens in 1.98 s; 1,048,576 in 43.3 s; 10,485,760 in 5.5 min
(7.35 GiB file). The drain is linear — per-command cost is flat (30–41 µs) and
the half-split ratio is flat (3.7–4.6) across three orders of magnitude — and
validation memory is working-set-bounded (~1.1 GiB peak at 10M, mapped-file
residency under reclaim, not accumulating heap). There is no algorithmic
pathology to fix. The problem is the product of two permanent commitments:
history never prunes, and every startup validates all of it (ADR-0019, which
also rejects clean-shutdown markers). A long-lived busy application (10⁸
commands) would take ~1 hour to open. Issue-tracker-class applications live
for decades.

## Decision

Two independent, composable mechanisms, both additive to the durable format
under registry-digest governance:

### 1. Event and commit retention behind a compaction watermark

A per-database durable compaction watermark (new `meta` key, precedent
ADR-0072) below which event payloads and commit-record bodies MAY be pruned.
The watermark MUST NOT advance past the minimum projection frontier
(reaffirming ADR-0082's constraint: commit materialization treats a missing
event row as corruption) and MUST NOT advance past any retention hold the
operator declares. Pruning replaces pruned rows with a per-range tombstone
digest chained from the registry digest so the sequence remains provably
contiguous: startup validates tombstone integrity for pruned ranges instead of
row-by-row content. Idempotency terminal records are retained independently
(the retry contract outlives event retention); an additive time-index enables
their separate policy later, per ADR-0082.

### 2. Validated-prefix checkpoint (the ADR-0019 amendment)

ADR-0019 rejects clean-shutdown markers because a forgeable marker converts
"validated" into "asserted." The amendment preserves that reasoning: the
checkpoint is not a marker but a **proof-carrying summary** — a digest over
the validated prefix (structural chain state, entity-chain map fingerprint,
per-table row counts and content digests at a commit-sequence boundary),
stored durably under the registry digest, recomputed and verified as the FIRST
act of the next startup by revalidating a randomly sampled window plus the
entire suffix after the checkpoint sequence. Startup cost becomes
O(suffix + sample + checkpoint-verify) instead of O(history). A checkpoint
that fails verification is discarded and full validation runs — fail-closed,
never fail-open. Checkpoints are written during shutdown AND opportunistically
at runtime (piggybacked on the writer lane's idle windows), so an unclean
crash loses at most the since-last-checkpoint suffix, bounding PERF-014.

### Interaction

Retention shrinks what full validation must cover; checkpointing makes even
retained history cheap to reopen. Either alone suffices for the measured
curve; together, startup is O(recent activity) regardless of database age.

## Consequences

- Startup at 10⁷+ commands drops from minutes to seconds (suffix-bound).
- ADR-0019's every-startup-validation guarantee is preserved in strengthened
  form: everything is validated either directly (suffix + sample) or by proof
  (checkpoint digest), and the proof is verified, not trusted.
- The durable format gains one meta key, one tombstone record schema, and one
  checkpoint record schema — all via the established migration chain.
- Projection health becomes retention-critical, with a bounded blast radius:
  a stalled projection fences the watermark only within its replay budget
  (max age / bytes / backlog, ADR-0086 §8); on breach it is marked
  RebuildRequired and detached rather than retaining the log indefinitely.
  Projection-frontier monitoring is a prerequisite deliverable.
- PERF-013 gains a checkpointed variant; the linear-drain evidence
  (startup-scale records) is the baseline both mechanisms are measured against.

## Rejected alternatives

- **Clean-shutdown marker.** Still rejected; asserts instead of proves.
- **Validation-free fast path.** Rejected; fail-open on corruption.
- **Compaction by rewriting history.** Rejected; ADR-0083 references and
  audit reciprocity assume stable sequences; tombstones preserve identity.

## Acceptance

Accepted by the maintainer on 2026-08-01.
Unresolved implementation detail (watermark advance cadence, sample policy,
tombstone schema) proceeds to package briefs after acceptance.

## Amendment 1 — checkpoint definitions and package split (Proposed 2026-08-02)

The checkpoint mechanism's durable key, encoding, bindings, crash meaning,
write points, and sample policy are defined by ADR-0019 Amendment 1, which
this amendment incorporates by reference. Two narrowings against the original
text, both fail-safe:

- **Sample policy (resolving the truncated sentence above):** sampled-window
  offsets are derived deterministically from the checkpoint's own self-hash
  (no entropy surface, auditable, unpredictable to an author who cannot
  already rewrite history); window count and size are fixed constants of the
  storage crate, chosen so sampling stays under one percent of a
  10-million-commit history.
- **Write points:** startup-validation completion and graceful shutdown
  only. Opportunistic runtime checkpoints are deferred until a writer-lane
  idle-window mechanism exists; the consequence is that an unclean crash
  revalidates the suffix since the last completed startup or graceful
  shutdown, which still bounds PERF-014 for any process that has ever
  completed one of the two.

**Package split.** The checkpoint ships first and alone (with the real
kill-mid-write PERF-014 evidence it bounds). Retention — watermark advance,
tombstone record schema, reciprocity-check tolerance, backup interaction,
provenance's non-sequence key, and the ADR-0086 §8 replay-budget scoping —
follows in its own package once its open decisions are put to the
maintainer; nothing in the checkpoint package forecloses any retention
choice.

Status: Proposed. Acceptance is the maintainer's.
