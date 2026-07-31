# ADR-0073: Linear Startup Validation and Bounded History Access

- **Status:** Accepted
- **Date:** 2026-07-30
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `PERF-003`, proposed `PERF-013`/`PERF-014`, `REC-003`
- **Related work packages:** Package H (pre-alpha hardening)
- **Amends:** ADR-0019 (validation cost, not validation scope), ADR-0021
  (service-audit lookup mechanics)

## Context

Several access paths grow super-linearly with retained history: the startup
structural evidence pass is quadratic (per-position iteration from row zero);
the historical evidence pass re-scans nine tables per emitted item; an
unpinned named-query request validates the entire audit table; audit
pagination re-iterates from row zero per page; the projection worker re-reads
the whole commit log every pass; the transient audit index retains every
request identity ever seen, is rebuilt by a full scan at every activation, and
is unbounded in memory; the capability view fails startup outright past its
bound. The newest restart-recovery evidence is 43.9 s at 1,024 commits —
super-quadratic — and the growth gate tests only to 4,096 commands.

ADR-0019 requires complete structural and catalog-semantic validation on every
startup with no fast-path exemption. That requirement is about *scope*, not
about implementation cost; nothing in it mandates quadratic access. Retention
and pruning remain out of scope (ADR-0004): this record bounds access paths
without deleting data.

## Decision

### Validation stays complete; its cost becomes linear

- The structural evidence pass holds one read transaction for the session and
  advances persistent per-table iterators; every row is still visited and
  decoded exactly once. Page continuity is guarded by O(1) recounts instead of
  per-position rescans.
- The historical evidence pass performs one forward scan per source table with
  the existing per-row validation, collapses plan references during the scan,
  builds one ordered locator index, and serves pages from it. The emitted
  evidence stream — content, order, and order keys — is unchanged. The index
  is bounded by a closed constant; exceeding it fails closed.
- ADR-0019's every-startup validation guarantee is reaffirmed verbatim.

### Hot paths stop scanning history

- The active query-module pointer is served from a process view populated at
  startup and activation (the same trust argument as the existing catalog
  view); the storage-port full-stream validator is unchanged but no longer on
  the request path.
- Audit pagination range-starts at the caller's position.
- Commit scans accept a resume position; the projection worker resumes from
  its frontier instead of sequence one.

### Service-audit request lookup becomes durable

Per-request audit-sequence lookup moves from an unbounded in-memory map to a
durable secondary index written atomically with each service audit append.
In-memory bounding was rejected as fail-open: nothing else enforces
per-request-identity uniqueness, so eviction would silently admit duplicate
request identities. Existing databases are migrated by a crash-restartable
backfill under a new registry digest. The duplicate-lifecycle gate is
unchanged; only where its evidence comes from changes.

### Caches are bounded and never fail startup

The capability view becomes a bounded cache with storage fallthrough (revision
monotonicity enforced for resident entries; integrity checks unchanged); the
known-absent digest set becomes a bounded LRU. Neither can fail startup by
growing.

### Recovery time becomes evidence

Crash repair keeps the one-phase commit profile (quick-repair is mutually
exclusive with it); the database open gains a repair progress callback for
observability. The growth benchmark extends to 65,536 retained commands and
records clean-startup and unclean-recovery wall times per checkpoint. Proposed
gates: startup growth sub-quadratic and bounded at the top checkpoint
(PERF-013), unclean recovery within a small factor of clean startup
(PERF-014) — enabled only after first measurements confirm the bounds.

## Consequences

- Startup, activation, and recovery costs become linear in retained history;
  the 43.9 s evidence is regenerated and replaced.
- One new durable table and registry digest step (sequenced after ADR-0072's
  migration).
- Memory no longer grows with lifetime traffic.
- The architecture tests freezing tail-versus-full-scan invariants are
  unchanged; hot-path caching happens strictly above the storage port.

## Rejected alternatives

- **Clean-shutdown markers or validation skipping.** Explicitly forbidden by
  ADR-0019 and not needed once validation is linear.
- **In-memory bounding of the audit request index.** Fail-open to duplicate
  request identities.
- **Retention/pruning.** Deferred; separate decision with its own format
  consequences.
- **Quick-repair.** Forces two-phase commit, conflicting with the accepted
  standard profile; recovery observability plus linear validation addresses
  the actual dominant cost.

## Acceptance

The human maintainer explicitly accepted this exact record on 2026-07-30 in
the current Claude session. Package H may merge against it.
