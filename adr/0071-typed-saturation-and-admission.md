# ADR-0071: Typed Saturation and Admission

- **Status:** Accepted
- **Date:** 2026-07-30
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `PERF-004`, `REC-001`, `ID-004`
- **Related work packages:** Package S (pre-alpha hardening)
- **Amends:** ADR-0007 (service admission boundary), ADR-0058/ADR-0060
  (scheduler admission semantics)

## Context

RiffDB has no distinct overload signal. Every capacity exhaustion — coordinator
queue, retained-byte budget, blocking-port bound, cursor and subscriber limits —
collapses into `RDB-STORAGE-0101`, indistinguishable from a storage fault. The
coordinator queue wait is unbounded below the request deadline, and a request
whose deadline elapses while queued returns a details-free deadline status that
the SDK classifies as non-retryable, while an equivalent capacity rejection is
retryable. Retained-byte rejection occurs after the final authorization safe
point, spending authorization work on requests that cannot be admitted.

Saturation behavior becomes a de-facto API the day agents build against alpha.
This record makes it an explicit typed contract.

## Decision

### Overload is a distinct, certain-not-executed, retryable rejection

- Kernel `PublicErrorKind::Overloaded` (`"overloaded"`) and application
  `RDB-CAPACITY-0101` (category `capacity`, recovery `retry`, fix
  `retry_later`) reject work the service did not admit. An overload rejection
  proves the command did not and will not execute.
- gRPC carriage is `ResourceExhausted` on both the kernel and application
  paths. `Unavailable` remains the transport-uncertainty signal; overload is
  never uncertain and never enters uncertainty recovery.

### Admission precedes authorization

Command capacity — queue depth and retained bytes — is checked before
re-authorization and preparation construction. The admission wait is bounded:
a non-blocking reservation, then at most the closed admission window
(150 ms) or the remaining request deadline minus a floor (25 ms), whichever is
smaller, then a typed overload rejection. The coordinator queue depth itself is
unchanged; depth becomes an explicit signal rather than an implied unbounded
wait.

### Deadline reclassification

A deadline that elapses while a request is queued but not admitted is a
capacity rejection (`RDB-CAPACITY-0101`), not a deadline error: the work
provably did not start. Deadline and cancellation semantics after admission
are unchanged, including uncertainty handling.

### One admission contract point

Transport-level concurrency limiting (tower middleware, HTTP/2 caps) is not
added: a transport limiter cannot emit typed application errors with operation
identity and would reintroduce unclassified rejection. The service edge is the
single admission contract point. MCP's existing session admission and rate
limits are unchanged and sit in front of this contract.

### Saturation is observable

Capacity rejections emit a dedicated telemetry event carrying operation,
ingress, and rejection stage (queue depth or retained bytes), without
high-cardinality labels.

## Consequences

- Clients and SDKs distinguish "back off and retry" from "storage fault" and
  from "outcome unknown"; agent retry loops become safe to write.
- Saturated servers reject quickly and cheaply instead of queueing
  authorization work they cannot admit.
- The details-free deadline path narrows to genuinely post-admission
  deadlines.
- Load evidence must demonstrate that at saturation, all rejections carry the
  typed code and accepted-work latency stays bounded.

## Rejected alternatives

- **Reuse `RDB-RESOURCE-0101`.** That code means "your result exceeds a
  bound → correct the request"; conflating it with "we are full → retry later"
  makes category-driven client handling ambiguous.
- **Transport-level load shedding.** Untyped, contextless, and duplicative of
  the service-edge contract.
- **Unbounded queue waits (status quo).** Converts saturation into deadline
  ambiguity and non-retryable client failures.

## Acceptance

The human maintainer explicitly accepted this exact record on 2026-07-30 in
the current Claude session. Package S may merge against it.
