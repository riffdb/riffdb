# ADR-0048: MCP Observer Physical-Call Accounting

- **Status:** Accepted
- **Direction approved:** 2026-07-23
- **Exact text accepted:** 2026-07-23
- **Accepted:** 2026-07-23
- **Acceptance reference:** Human maintainer confirmation in the current Codex
  session on 2026-07-23
- **Requires:** ADR-0007, ADR-0008, ADR-0027, ADR-0040, and ADR-0046
- **Amends:** ADR-0008's observer-call terminology and exact per-tick/session
  physical-call bounds; SPEC Sections 12.8 and 12.12; WP-140
- **Decision deadline:** Before WP-140 observer fixtures and transport
  conformance are accepted

The human maintainer accepted this correction after implementation exposed that
one high-level command-plan observation performs a bounded sequence of public
service calls. This record separates logical observer operations from physical
service calls and makes both limits executable.

## Context

ADR-0008 counts at most two compact discovery operations plus eight subscribed
resource operations per unchanged tick, and at most six compact discovery
operations plus eight subscribed resource operations per changed tick. It calls
the resulting `14 * 180 = 2,520` bound "background service calls."

That terminology is incorrect for command-plan subscriptions. One command-plan
operation performs up to five physical calls:

1. up to three compact `DiscoverResources` pages;
2. one `ExplainCommand`; and
3. one final descriptor-fenced compact `DiscoverResources` call.

The common observer driver correctly bounds high-level work to fourteen logical
operations per tick. It does not, by itself, bound the physical service calls
inside those operations. A stdio-only parity probe also performed unrelated
inventory calls outside that accounting.

## Decision

The existing common bound is renamed, not removed:

- at most fourteen watcher-generated logical observer operations per tick; and
- at most 2,520 logical observer operations over 180 ticks.

Physical service work has a separate exact conservative bound:

- one compact discovery operation permits at most one physical service call;
- one subscribed-resource operation permits at most five physical service
  calls;
- a changed tick therefore permits at most
  `6 * 1 + 8 * 5 = 46` physical service calls; and
- one 900-second session permits at most `46 * 180 = 8,280`
  watcher-generated physical service calls.

Each session owns one common physical-call meter. A compact discovery operation
receives a one-call handle and a subscribed-resource operation receives a
five-call handle. The transport-specific backend charges that handle
immediately before each public gRPC or API-neutral application-service dispatch.
A charge is never refunded after dispatch is attempted, including transport,
authentication, authorization, cancellation, or response-validation failure.
Attempt 8,281, or an operation-local excess, fails closed and terminates the
observer session.

Hosted production composition charges the unique
`HostedObserverServiceCaller` bridge immediately before it hands one request to
the freshly authenticated response-body dispatcher. Stdio charges immediately
before every public SDK call. Generic test/conformance backends remain isolated
from production composition and must use an explicit metered fixture when they
claim physical-call evidence.

The stdio parity probe is removed. Every stdio operation already uses the same
public gRPC surface, and discovery/resource calls validate the exact response
variant, fence, cursor, URI, schema identity, and bounded content they consume.
An extra process-wide inventory scan adds work but grants no authority or
semantic parity evidence.

Client-originated MCP work is not charged to this watcher meter. It remains
subject to the existing request, rate, in-flight, and response limits.
Notifications make no service call and do not affect the meter.

## Options Considered

1. **Keep 2,520 and describe logical calls as physical service calls:** Rejected.
   It leaves command-plan expansion unbounded by the stated invariant.
2. **Charge five calls for every subscription whether dispatched or not:**
   Rejected. Reservation is conservative but obscures actual work and exhausts
   a session for calls that never occurred.
3. **Batch or add a dedicated observation RPC:** Deferred. It could lower the
   bound but would add a public semantic operation and broaden WP-137/WP-140.
4. **Count only in each transport:** Rejected. A common meter type and operation
   handles keep the invariant and boundary tests identical while dispatch
   remains transport-owned.

## Consequences

- Logical scheduling and coalescing behavior remains unchanged.
- The maximum admitted watcher-generated physical load is explicit and
  mechanically enforced.
- Worst-case physical work is higher than the earlier mislabeled number.
- Every new multi-call observed resource must fit the five-call operation
  handle or require a separately accepted amendment.
- Batching and a lower physical-call ceiling remain post-POC optimization work.

## Compatibility

This changes no MCP method, tool, resource URI, schema, public Protobuf message,
public SDK interface, contract grammar, typed IR, plan hash, canonical input
hash, durable record, storage key, or database migration. It corrects an
operational limit and internal accounting terminology before POC acceptance.

## Security

Charging before dispatch prevents authentication failures, cancellation races,
and malformed responses from creating unmetered retry work. The meter contains
only bounded counters and no principal, credential, request, policy, resource
content, or error detail. Exhaustion closes the observer without exposing which
resource or lower call consumed the budget.

## Testing

- Common boundary tests freeze one/five-call operation handles, no refund, the
  46-call tick calculation, exact acceptance of 8,280, and rejection of 8,281.
- Hosted tests prove every production observer service bridge dispatch charges
  the session meter and notifications do not.
- Stdio tests prove every observer public SDK dispatch charges the same meter,
  command-plan observation cannot exceed five, and the removed parity probe
  cannot reappear through source architecture checks.
- Observer schedule tests retain the separate fourteen-operation and
  2,520-operation assertions.

## Requirements and Work Packages

- **Requirements:** `MCP-032`, `MCP-041`, `MCP-042`, `MCP-043`, `MCP-049`
- **Defines or blocks:** `WP-140`
- **Final evidence:** `WP-140`, `WP-200`

## Decision Deadline

The exact text was accepted before WP-140 observer fixtures and conformance
evidence were accepted.
