# ADR-0182: Bounded Dirty Recovery and Online Retention

- **Status:** Accepted
- **Obligations:**
  - `OBL-0182-1` WP-760 must prove that dirty startup reaches readiness
    after journal-suffix recovery and bounded-root validation with no
    entity, index, history, event, audit, provenance, projection, outbox, or
    idempotency population walk and within the compiled recovery heap
    ceiling; planned proof
    `dirty_startup_reaches_readiness_without_a_population_walk`.
  - `OBL-0182-2` WP-760 must prove that a corrupt row outside the bounded
    roots fails closed with the existing typed corruption outcome on first
    access after a dirty start, and is never reported as absence, a business
    outcome, partial output, or repaired state; planned proof
    `planted_row_corruption_fails_closed_on_first_access_after_dirty_startup`.
  - `OBL-0182-3` WP-760 must prove that the background scrub exposes only
    the closed health states, honors its rate bound and cancellation, and
    records a failure as an incident without repairing or hiding the
    affected row; planned proof
    `background_scrub_health_transitions_are_closed_and_cancellable`.
  - `OBL-0182-4` WP-760 must prove that the authorized public offline scrub
    drives the complete exact-end structural and catalog-semantic path and
    emits a versioned receipt; planned proof
    `public_offline_scrub_drives_the_complete_validation_path`.
  - `OBL-0182-5` WP-761 must prove that online retention advances the
    watermark only under the complete fencing set, in bounded steps, through
    the sole ordered writer, and every crash window at a step boundary
    recovers to a valid watermark, tombstone chain, and allocator state;
    planned proof
    `online_retention_crash_windows_recover_to_a_valid_watermark`.
  - `OBL-0182-6` WP-760 must prove that release evidence reports
    dirty-restart wall time and peak heap separately at the 65,536-row and
    production-scale checkpoints, scrub throughput, and online-prune
    interference with 32-client p95; planned proof
    `check-dirty-recovery-evidence`.
- **Direction approved:** 2026-09-01
- **Exact text accepted:** Yes, 2026-09-01
- **Accepted:** 2026-09-01
- **Acceptance reference:** Maintainer acceptance of the exact text in the
  current Claude Code session on 2026-09-01, all seven consolidation records together
- **Decision deadline:** Before WP-760 changes the dirty startup readiness
  path or WP-761 advances a retention watermark while an operational writer
  is running
- **Requires:** ADR-0019, ADR-0050, ADR-0061, ADR-0072, ADR-0073, ADR-0085
  (Amendments 1 through 4), ADR-0093, ADR-0101, ADR-0103, ADR-0104, ADR-0112,
  ADR-0156 (Amendment 3), ADR-0157, ADR-0163, and ADR-0165
- **Amends:** ADR-0019's unconditional complete pass on the dirty path;
  ADR-0073's "validation stays complete" rule for readiness; ADR-0085
  Amendment 2's offline-exclusive prune execution and its "never in the hot
  write path" advancement rule; ADR-0156 sections 6 and 7 where they require
  the complete exact-end path before dirty readiness; `REC-004`; the dirty
  ordering of SPEC section 10.6; and the evidence shape of `PERF-014`
- **Defines or blocks:** WP-760 through WP-761

The maintainer accepted the exact text of this record on 2026-09-01. Its packages
may begin. Each deferred obligation above is tracked in
`adr/obligations-outstanding.yaml` until its planned proof exists, at which
point the owning package discharges it by declaring the proof.

## Context

ADR-0156 and ADR-0165 made a clean restart bounded. Commit `a71c03cb` landed
bounded readiness at 1.35 seconds where the previous complete path took 34.75
seconds at 115,690 commands. That improvement applies only when the prior
process wrote a valid clean-close certificate. ADR-0156 section 6 and `REC-004`
require every other startup to execute the existing complete exact-end
structural and catalog-semantic path, and `docs/known-limitations.md` records
that dirty restart "still performs complete startup validation".

The complete path is proportional to live state. ADR-0156's own context
records a production-scale database of about 1.1 million entities on which
the historical evidence locator index exceeded its 512 MiB heap ceiling and a
bounded implementation still inspected millions of live entity and index
rows. `crates/riffdb-storage-redb/src/startup.rs` is 13,453 lines and owns
that walk. For a hosted multi-tenant service the dirty path is the one that
runs after every host failure, so the recovery-time objective is set by the
path that does not scale.

History growth has the same shape. ADR-0085 Amendment 2 prunes commit bodies,
event payloads, and outbox rows below a watermark, but only through an offline
exclusive maintenance operation on a closed database file
(`crates/riffdb-storage-redb/src/retention.rs`, `riffdb retention prune`), and
Amendment 4 added a journal rebase ceremony because that operation opens redb
directly. Between maintenance windows the commit log, events, provenance, and
outbox grow without bound. Backups are offline for the same reason.

ADR-0093 makes both properties prerequisites rather than conveniences. A
follower bootstraps from a backup or a full sync and then passes primary
startup validation at its applied head; a lagging follower's acknowledged
frontier becomes a retention fencing input. Neither works at production scale
while dirty recovery walks the population and retention requires exclusive
offline access.

ADR-0156 already adopted the conventional local-database trust model for the
clean path: recovery is distinct from exhaustive verification, latent media
corruption is detected by checksums and validation where the data is touched,
and an explicit scrub exists for complete assurance. This record extends that
model to the dirty path and moves retention into the running process. It does
not weaken atomic acknowledgement, crash recovery, idempotency, authorization,
schema safety, fail-closed handling of malformed evidence, or any durable
format.

## Decision

### 1. Dirty recovery is journal-suffix replay plus bounded-root validation

A startup without a valid clean-close certificate performs, in order: engine
open and durable-format identity checks (`AFC-002`); recovery of every
complete durable journal frame into redb under the ADR-0101 through ADR-0104
checkpoint-plus-suffix rules; the same bounded-root validation ADR-0156
section 5 requires of the clean path; consumption of a dirty next generation
per `REC-004`; and readiness. No population-sized table is enumerated before
readiness. The complete exact-end structural and historical evidence stream
is not constructed on the readiness path.

Startup mode reporting gains no new public value: health continues to report
the closed mode `clean_certificate` or `complete_validation`, and the latter
now names the bounded dirty path. The explicit complete path of section 4
reports its own receipt.

### 2. Rows outside the bounded roots are validated on first access

Every operational path that reads or mutates a row validates its versioned
envelope, checksum, canonical encoding, physical-key identity, schema
ownership, bounds, and locally required reciprocal evidence before the row
influences authorization, policy, execution, output, delivery, projection, or
mutation. ADR-0156 section 5 and ADR-0165 already require this for the clean
path; this record makes it the sole pre-access guarantee for the dirty path
as well. A row that fails validation produces the existing typed corruption
outcome and an internal integrity incident. Corruption discovered after
readiness is never converted into absence, a business outcome, partial
output, skipped work, or automatic repair.

### 3. A background complete scrub runs after readiness

After readiness the server starts one background scrub that drives the
complete exact-end structural and catalog-semantic validation over a
least-authority MVCC reader. The scrub exposes exactly the closed health
states `pending`, `running`, `complete`, `failed`, and `cancelled` through a
new `scrub` component beside the existing authoritative components. It runs
under a bounded work rate expressed in rows per scheduling step, never in
wall-clock terms, and is cancelled by shutdown. A failed scrub degrades the
`Storage` component with an incident identifier and leaves every affected row
to fail closed on access; it does not repair, hide, or retry past the
incident. Scrub progress is process-local; a restart restarts the scrub.

### 4. The offline scrub becomes a public authorized operation

The complete path remains authoritative for operators who require full
assurance before serving. It is exposed as one authorized offline maintenance
operation on the existing ADR-0050 service path, surfaced as
`riffdb storage scrub`, driving the same exact-end path over a closed
database and emitting a versioned bounded receipt beneath `.maintenance`.
Backup verification, restore, format upgrade, and retention preflight may
request the same path. A successful scrub does not itself create a clean
certificate; ADR-0156 section 6 is unchanged on that point.

### 5. Recovery memory is bounded by construction

No recovery stage may allocate a population-proportional structure. Every
recovery-owned structure is proportional to the journal suffix, to the
bounded roots, or is a fixed-size streaming window. Recovery-owned state
beyond engine caches is limited to 64 MiB; a stage that would exceed that
ceiling fails closed with a typed internal defect rather than spilling or
degrading. The historical evidence cursor remains memory-bounded because the
scrub and the explicit complete path still use it.

### 6. Retention runs online on a background maintenance lane

Watermark advancement, prune, and tombstone verification run inside the
running process on a background maintenance lane. The lane holds no exclusive
lease and opens no second writer: each prune step is one bounded
administration-class durable transition submitted to the sole ordered writer,
so `PERF-007` holds by construction and the step is journaled like every
other transition. Each step atomically deletes one sub-range of at most 256
sequences, appends its tombstone, and advances the watermark, exactly as
ADR-0085 Amendment 2 defines for the offline sub-range. The first step
deletes any validated-prefix checkpoint. The ADR-0085 Amendment 4 journal
rebase is unnecessary online because the process has already recovered its
journal and the writer is live; the offline operation retains Amendment 4.

The fencing set is the complete existing set (projection frontiers, durable
consumer low water, undelivered-outbox low water, staged migration frontier,
operator holds) plus every registered follower's acknowledged frontier under
its ADR-0093 section 4 hold budget. An unreadable, unvalidated, or
incarnation-mismatched fencing source refuses advancement. The lane's target
is configured in `riffdbd` as a retained-sequence count or a hold, never a
wall-clock age. Application principals have no operation that starts, stops,
or targets retention; the existing audited hold, detach, and reattach verbs
are unchanged.

### 7. Interference and progress are bounded

The writer is blocked by at most one prune step at a time. The lane yields
after every step and advances at most a configured number of sequences per
durability epoch. Release evidence measures 32-client interactive p95 with
the lane active against the same run with it idle; the lane is acceptable
only inside the existing five-percent no-regression bound.

### 8. Evidence

Release evidence reports separately, in the `PERF-019` style: engine open,
journal recovery, bounded-root validation, dirty-generation consumption,
readiness, and total wall time for dirty restart at the 65,536-row and
production-scale checkpoints, together with peak recovery-owned heap; scrub
rows per second and total scrub time on the same checkpoints; and the
online-prune interference measurement of section 7. `PERF-014` continues to
require a genuine writer kill and continues to reject relabelled clean
drains.

## Options Considered

1. **Keep complete validation on every dirty start.** Rejected. Restart time
   is proportional to live state, the million-entity reproduction exhausted a
   512 MiB heap ceiling, and every host failure in a hosted service takes the
   dirty path.
2. **Rely on the ADR-0085 validated-prefix checkpoint alone.** Rejected. The
   checkpoint shortens the history walk but ADR-0085 explicitly retains full
   passes over current entities, indexes, catalog, and capabilities, which is
   the population-proportional part.
3. **Give online retention its own lease and writer.** Rejected. A second
   writer violates `PERF-007` and reintroduces the exclusive-access and
   journal-rebase machinery this record removes; the ordered lane already
   serializes bounded administration transitions.
4. **Time-based retention.** Rejected. The architecture forbids clock
   authority; retention targets are sequences and holds, which is also the
   vocabulary ADR-0093 uses for follower lag.

## Consequences

- Dirty restart becomes proportional to the journal suffix plus bounded roots
  and holds a fixed heap ceiling, which is the recovery-time property a
  hosted service and an ADR-0093 follower bootstrap require.
- Exhaustive corruption discovery on the dirty path moves from before
  readiness to first access plus the background scrub; ADR-0156 already
  accepted this timing for the clean path.
- History is compacted without downtime and without an exclusive lease, and
  the retention fence gains follower frontiers.
- Cost: the scrub consumes reader capacity after every dirty start; the
  prune lane takes a bounded share of the writer.
- Deferred: acknowledgement semantics (ADR-0061), durable formats, the
  hardened profile, provenance pruning (ADR-0085 Amendment 2 deferral), and
  online backup, which ADR-0178 derives from the changelog stream.

## Compatibility

No durable record, storage key, journal frame, backup manifest, tombstone, or
watermark format changes. The offline retention operation and its receipts
are unchanged and remain available. Public surfaces gain one authorized
offline maintenance operation and its receipt family, one closed health
component, and one `riffdbd` configuration section; no gRPC message, MCP tool,
generated client, contract IR, or query artifact changes. `REC-004`'s
complete-path sentence is replaced by the bounded dirty path of section 1.

## Security

No new authority is introduced. The public scrub uses the ADR-0050
policy-owned authorization proof and receives no storage handle or path. The
online lane is driven only by configuration and by the existing audited
retention verbs; no application principal can start, stop, target, or observe
it beyond the closed health states. Health and diagnostics expose closed
states, bounded timings, and incident identifiers only, never hashes,
frontiers, counts, paths, or keys. Follower fencing inputs arrive through the
ADR-0093 replication capability and are validated like every other fencing
source. Fail-closed handling of malformed evidence is unchanged.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** No. No public request,
  contract, flag, or transport option can skip validation, force readiness,
  suppress the scrub, acknowledge an unsafe write, or advance retention past
  a fence. The only additions are an authorized operator operation that
  performs more validation and a closed health component.
- **Scale:** This record removes two single-node full-state assumptions: the
  dirty path no longer rewrites or walks full state before serving, and
  retention no longer requires exclusive whole-database access. Follower
  frontiers enter the fencing set so the same rules hold with replicas. The
  64 MiB recovery ceiling is a named, measured, reversible constraint.

## Testing

- `dirty_startup_reaches_readiness_without_a_population_walk`: a real-daemon
  kill and reopen at the 65,536-row checkpoint pins the readiness path's
  table enumeration count and peak recovery heap.
- `planted_row_corruption_fails_closed_on_first_access_after_dirty_startup`:
  a planted envelope, checksum, and reciprocity corruption in each population
  row class reaches readiness and then fails closed on the first read,
  mutation, delivery, projection, and export touch.
- `background_scrub_health_transitions_are_closed_and_cancellable`: state
  transitions, rate bound, shutdown cancellation, and a planted failure that
  degrades with an incident and repairs nothing.
- `public_offline_scrub_drives_the_complete_validation_path`: receipt,
  authorization refusal, and equivalence with the retained exact-end path.
- `online_retention_crash_windows_recover_to_a_valid_watermark`: process
  kills and `riffdb-sim` seeded schedules before, during, and after each
  step's delete, tombstone append, watermark advance, and checkpoint deletion;
  fencing refusal for each unreadable or mismatched source including a
  registered follower.
- `check-dirty-recovery-evidence`: the release evidence validator for
  section 8.
- The existing `storage_recovery_matrix`, `full_recovery_matrix`, and
  `offline_maintenance_recovery` arms remain and gain the dirty bounded path.

## Requirements and Work Packages

- **Requirements:** `REC-005` through `REC-008` and `STO-030` through
  `STO-032`
- **Defines or blocks:** WP-760 through WP-761
- **Final evidence:** WP-761

## Decision Deadline

Exact acceptance is required before WP-760 changes which validation runs
before dirty readiness and before WP-761 commits a prune step from a running
process. Until then the complete dirty path and the offline-exclusive
retention operation remain authoritative.
