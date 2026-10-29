---
adr: "0323"
title: Request-Scoped Native Dependency Admission Before Durable Audit
status: accepted
tier: guarantee
date: 2026-10-16
accepted: "2026-10-16"
acceptance: 'maintainer, in session, 2026-10-16: "they are approved" (ADR-0315, SPEC section 13.5 and WP-825 registration)'
requires: [ADR-0007, ADR-0071, ADR-0327]
amends:
  - ADR-0007 service admission and SPEC section 13.5 audit entry boundary
  - ADR-0071 capacity-only unaudited pre-admission refusal
  - ADR-0327 Decision 7 pending native locator admission
supersedes: []
requirements: [API-001, SEC-001, SEC-005, REC-001, TXN-042]
packages: [WP-825]
obligations: []
review_triggers:
  - An initial dependency refusal could occur after any operation or audit work is accepted.
  - A pending or invalid directory could establish absence or attach to another source.
  - An accepted audit failure, authorization rule, admission deadline or compiled ceiling would change.
  - Follower, archive/static, pre-bootstrap health or recovery-only authority would change.
---
# ADR-0323: Request-Scoped Native Dependency Admission Before Durable Audit

## Context

Bounded restored startup can correctly reach Ready while its exact native
locator generation is Pending. The first acknowledged-command replay then
receives typed storage unavailable. Its required terminal audit also needs that
generation; an accepted audit failure correctly stops authoritative service.
A proposed service admission wait must prevent this healthy construction state
from being admitted as an audit failure, without postponing bounded readiness.

SPEC section 13.5 and ADR-0007 audit every structurally valid authenticated
mutation and administrative read, including prestart cancellation. ADR-0071's
unaudited pre-admission exception covers capacity only. Extending that exception
to a specifically unadmitted native dependency requires exact human acceptance;
it cannot be inferred from ADR-0327's permission to refuse pending locators.

## Decision

1. Add one first-party, API-neutral initial native dependency admission phase
   to the shared service. It precedes operation execution/classification,
   idempotency inspection, durable audit input construction, coordinator ticket
   submission, sequence allocation and protected-result release. Callers cannot
   request, omit or bypass it. Transport authentication and structural request
   validation retain their existing boundaries. This phase grants no authorization.
2. Apply the phase to every ordinary authenticated request entering the
   live-source service composition that owns command/control/audit writers,
   including standard reads and authenticated operational health. Any such
   request can require audit on denial, cancellation or a policy obligation;
   an operation-name classifier cannot safely exempt it before policy runs.
   The native metadata-only ports, existing bounded Ready signal, pre-bootstrap
   health context and recovery-only host retain their existing authority and
   bounded availability. No new unaudited health route is introduced. Follower
   refusal/read authority and archive/static reader composition retain their
   existing boundaries; they do not acquire a live-source audit writer.
3. A complete current-source capability admits immediately, including when the
   request is already cancelled or expired; its ordinary prestart audit rules
   then apply. A Pending capability may wait only on its admitted first-party construction owner's
   completion notification, under the request's existing absolute
   `RequestControl` deadline and cancellation signal. Reuse the existing
   cancellation/deadline scheduler; add no independent wait duration, new
   global deadline, timeout increase or polling/retry loop. No population walk
   runs in the request or Ready path. An unavailable construction owner or
   unadmittable Pending waiter produces existing typed unavailable. Invalid or
   failed proof retains existing integrity/corruption refusal and systemic health
   handling; it is never admitted or disguised as healthy Pending construction.
4. Only a request whose initial admission identified Pending may use this new
   exception, and only while zero operation, durable audit, coordinator ticket
   and authoritative mutation work was accepted. In exactly that state, amend
   SPEC section 13.5 and ADR-0007/ADR-0071: return the current
   Cancelled, DeadlineExceeded or typed Unavailable result without a durable
   service-audit row, sequence or coordinator ticket. It is not Overloaded,
   an application result, a policy denial, an idempotency answer or uncertainty
   recovery. Bounded redacted telemetry may record only the closed admission
   reason and operation/ingress class; no request values are recorded.
5. Immediately before leaving initial admission, prove that the current native
   source has a complete capability bound to its database, incarnation,
   registry, exact membership and source-root identity. A completed older pin
   cannot authorize a different current source. Source changes require the
   existing exact continuity proof or continued admission under the original
   request deadline; they never grant a fresh deadline or operation retry.
   Pending/partial state cannot prove absence. Retain only charged source/
   completion ownership needed for this check, never an unrelated future root.
6. Waiting holds no writer/mutation lease, coordinator actor, audit permit,
   index lock or mutable service state. Admit the waiter descriptor, completion
   subscription and retained source ownership under existing finite limits
   before allocation. Native locator ownership remains within ADR-0327's
   512-MiB aggregate, 448-MiB ordinary and 1,024-owner ceilings; existing provider,
   scratch, queue and payload-cache limits remain unchanged. Cancellation,
   deadline, refusal and completion release waiter ownership in bounded work.
   Cancelling a waiter cannot discard another reader's source, fabricate
   completion, or cancel shared construction still needed by live ownership.
7. After dependency admission, perform fresh current authorization and all
   existing request classification, catalog/idempotency, safe-point and commit
   checks. Waiting does not cache an allow decision, reusable policy proof,
   selected plan or absent outcome. The unchanged coordinator remains the sole
   sequence allocator and authoritative writer; transaction atomicity, ordering,
   uncertainty, provenance, scoping and redaction remain mandatory.
8. This exception ends irrevocably when operation/audit work is admitted.
   Thereafter existing Started/terminal audit, cancellation, containment,
   completion ownership and fail-closed behavior apply. An accepted audit error
   still stops or fences authoritative service as before; no storage error is
   ignored or reclassified to hide an audit outage. A real admission failure
   after that boundary cannot be relabeled as an initial dependency refusal.
9. Keep bounded daemon Ready, ordinary background scrub, original source
   construction and all recovery/administrative gates unchanged. The first
   public request may consume only its existing deadline; the 45-second release,
   60-second recovery and public administrative limits are not relaxed. Harness
   retries, delayed requests, delayed readiness and altered pacing cannot prove
   this change. If actual construction cannot meet the original workload gates,
   qualification still fails and the native limitation must be reported.

## Options considered

- **Population rebuild before Ready:** rejected because it defeats bounded
  startup and shifts the same unbounded population work into readiness.
- **Harness retry, delayed first replay or a longer timeout:** rejected because
  it masks the native lifecycle failure and changes the measurement.
- **Ignore accepted audit failure or treat Pending as absence:** rejected
  because it weakens durable audit or exact idempotency guarantees.
- **Unbounded writer-side wait:** rejected because it blocks construction's
  publication lease and has no request cancellation/deadline ownership.

## Consequences

A narrowly refused initial dependency attempt can now lack a durable audit row,
including cancellation of an otherwise structurally valid authenticated request.
That explicit guarantee change is limited to work never accepted by the service.
Ready remains bounded; Pending no longer needs to become an accepted audit error.
Ordinary authenticated health requests may wait at this same initial gate;
only the existing pre-bootstrap/recovery availability surfaces are exempt.
This does not guarantee that an arbitrary protected population fits the budgets
or completes before every request deadline.

## Standing design tests

- **Interface safety:** no application flag, unsafe handle, audit opt-out,
  weakened authorization or retry instruction is added. The service exclusively
  owns admission, and only proven unadmitted work can use this exception.
- **Scale:** completion notification and charged waiter/source ownership are
  bounded; no per-request history scan, full-index copy or retained unbounded
  predecessor chain is permitted. Native rebuild remains a single-node derived
  POC mechanism; this decision adds no population-dependent readiness work.

## Checks

- Deterministically hold a genuine restored current-source builder Pending;
  issue the original acknowledged replay immediately; prove zero operation/
  audit submissions, no writer lease held and no runtime stop while pending;
  complete the exact source and verify the same request's original outcome,
  sequence and provenance with its required successful audit lifecycle.
- Cancel and expire requests while Pending; prove current typed outcomes,
  zero durable audit/sequence/coordinator work, bounded waiter release and
  independent construction/protected-owner survival. Use notifications/barriers,
  not sleeps. Check completion-before-subscription and source-change schedules.
- Preserve fresh policy denial/revocation after the wait, invalid-completion
  refusal, no false absence, all ingress parity and follower/archive boundaries.
- Inject a genuine accepted audit storage failure and unknown commit status;
  prove unchanged stop/fence and no retry. Run closest native/service regression,
  actual first replay after public restore, scoped acceptance and complete CI.
