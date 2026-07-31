# ADR-0070: Read Stability and Internal Retry

- **Status:** Accepted
- **Date:** 2026-07-30
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `PERF-001`, `PERF-002`, `SAFE-*` read conformance, `TXN-001`
- **Related work packages:** Package R (pre-alpha hardening)
- **Amends:** ADR-0007 (service audit boundary, readiness trigger scope), ADR-0035
  (scan-fence observation handling), ADR-0053 (composite snapshot cursors)

## Context

Under ordinary concurrent write load, public application reads currently fail
with `RDB-STORAGE-0101`. Three verified mechanisms produce this: continuation
cursors are minted for every exactly-full page and registered without
replacement, so a single principal exhausts its 64-cursor budget and the read
is rejected; audit, outbox, and projection writers hold the process storage
cell in write mode while queued behind the redb mutation gate, starving every
reader until the blocking-port bound rejects instantly; and one command's
audit-append failure permanently poisons global routing, converting every
subsequent read into `RDB-STORAGE-0101`. Additionally, the query executor
collapses integrity, resource, and availability faults into one public code.

A database whose reads fail because writes are in progress is unusable for the
agent applications the alpha targets. This record fixes the read-path contract
before alpha freezes it.

## Decision

### Reads observe one snapshot and do not fail due to concurrent commits

An application read (named RiffQL query, `GetEntity`, `ScanIndex`) evaluates
against one storage snapshot. Concurrent command commits MUST NOT cause the
read to fail, block indefinitely, or observe torn state. Transient conditions
arising from concurrency are resolved inside the service:

- The retryable transient set is closed: backend unavailability, blocking-port
  admission unavailability, and cursor-registration transients. Nothing else
  is retried.
- Internal retry is bounded: at most 3 attempts, bounded backoff through the
  request-deadline scheduler, abandoned when the request is cancelled or the
  remaining deadline cannot fit another attempt.
- A stale continuation cursor is NOT a transient: internally re-running a
  cursored page could silently change the returned rows. It remains the
  client-visible `RDB-CURSOR-0101` with restart-from-first-page guidance.

### Continuation cursors are single-live and never reject a read

- A page that ends exactly at the end of its range mints no continuation.
  Continuations exist only when a further row was observed.
- At most one live continuation exists per principal per exact query identity.
  Publishing a new continuation supersedes the previous one; the superseded
  token thereafter resolves `RDB-CURSOR-0101`. Replacement occurs only at
  publication, never at registration, so a failed invocation cannot destroy a
  client's valid cursor.
- Cursor capacity is enforced by eviction (oldest for the principal at the
  per-principal bound; oldest overall at the global bound), never by rejecting
  the read. An evicted token resolves `RDB-CURSOR-0101`.

### Readers are independent of writer serialization

Pure-read storage access uses shared handles that take no process-wide lock.
The mutation gate serializes writers only. No component may hold a
process-wide reader-visible lock while waiting on the mutation gate.

Blocking-port admission for reads uses a bounded, deadline-aware wait with an
explicit waiter cap rather than instant rejection at the in-flight bound.
Cancellation and deadline classifications are preserved end to end.

### Error classes are separated

Integrity faults (corrupt or invariant-violating durable state) surface as
internal-defect errors; resource faults as resource errors; only genuine
availability faults surface as `RDB-STORAGE-0101`, and only after the internal
retry budget is exhausted.

### Audit readiness failure is scoped

Every command and audited operation still fails closed when its own audit
append fails. Global routing stops immediately only for subsystem-level causes
(coordinator stopped, draining, fenced, or an unknown audit outcome).
Request-scoped causes (deadline, cancellation, capacity) fail the affected
operation and increment a consecutive-failure count; any success resets it;
crossing the closed threshold (8) stops routing. Once stopped, routing remains
stopped; recoverability of the stopped state is unchanged and out of scope.

## Consequences

- Clients never see transient concurrency errors from reads; they see success,
  a real typed error, or their own deadline.
- Cursor semantics tighten: superseded and evicted continuations resolve
  `RDB-CURSOR-0101` instead of remaining silently resolvable.
- Read latency under writer pressure may rise by the bounded wait and retry
  budget; it no longer converts into spurious errors.
- One slow audit append no longer converts the entire process into a permanent
  `RDB-STORAGE-0101` responder.

## Rejected alternatives

- **Surface a typed retryable error and let clients retry.** Pushes the burden
  onto every client and agent; violates the alpha goal that reads are
  dependable primitives.
- **Retry stale cursors internally.** Changes returned rows silently; a
  correctness violation.
- **Make routing readiness recoverable.** Fail-closed monotonicity is retained;
  only the trigger is scoped.
- **Remove read-path audit obligations.** Audit semantics are unchanged by
  this record.

## Acceptance

The human maintainer explicitly accepted this exact record on 2026-07-30 in
the current Claude session. Package R may merge against it.
