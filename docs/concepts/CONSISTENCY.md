# Consistency and Recovery

RiffDB exposes the boundaries that application code usually has to infer from
separate transaction, queue, and projection systems.

## Authoritative state

Entity state, terminal outcomes, durable events, provenance, and the commit log
are authoritative. One successful command publishes them as one atomic graph.
If restart recovery observes incomplete or contradictory durable evidence, the
database fails closed rather than guessing.

## Idempotent uncertainty recovery

An idempotency key identifies one caller-intended command. The canonical input
hash prevents that identity from being reused for different input. A terminal
outcome survives restart, so a caller can distinguish a known committed result
from `OutcomeUnknown` after retry capacity is exhausted.

## Provenance

Each mutation records the authenticated actor, ingress, request and
idempotency identities, contract and plan identities, logical inputs, and the
resulting commit. Public responses and logs are redacted before emission;
internal sources are correlated with bounded incident IDs rather than exposed.

## Durable effects

Commands record effect intent in the same atomic commit as state. Connectors
deliver from the outbox after commit and must tolerate duplicate external
delivery. The database does not claim that an unreliable external system is
inside its transaction.

## Projection frontiers

A projection frontier is the greatest contiguous authoritative commit applied
to a projection generation. Callers that need read-after-commit behavior pass a
known commit sequence and wait within a bounded deadline. A projection can be
rebuilt from authoritative history without changing command truth.

## Admission-head reads

When a caller needs a stronger read but does not possess a commit sequence, it
can request `AdmissionHead`. After initial authorization, RiffDB captures the
authoritative application head once and serves only a snapshot or provider
epoch at or beyond that floor. A later concurrent commit is not required, so
this is a precise fresh-through-admission guarantee rather than linearizability.

The wait is bounded by the request and server freshness limits. Lag, rebuild,
retirement, cancellation, and saturation return typed failures; RiffDB never
falls back to a stale result. The first page stores the consistency class and
floor in opaque server cursor state. Continuations reuse that state without
capturing a moving head, and a non-fenced cursor cannot be upgraded in place.
An explicit read-after-commit fence still applies, with the effective floor set
to the maximum of all applicable fences.

## Concurrency

Logical conflict capabilities reduce avoidable races but do not replace
transaction-current validation. Exact entity versions, index epochs, and
predicate dependencies are rechecked immediately before commit. This is what
protects observations that influenced an outcome even when the observed row was
not mutated.
