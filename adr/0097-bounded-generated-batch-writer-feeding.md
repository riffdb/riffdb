# ADR-0097: Bounded Generated-Batch Writer Feeding

- **Status:** Accepted
- **Date:** 2026-08-05
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `AAA-006`, `AAA-007`, `AAA-010`, `PYD-005`,
  `PYD-012`, `PERF-004`, `PERF-005`, `PERF-006`, `PERF-008`, `PERF-009`,
  `PERF-010`
- **Related work packages:** `WP-457`
- **Amends:** ADR-0056, ADR-0058, ADR-0059, ADR-0060, ADR-0084, ADR-0096
- **Amended by:** ADR-0098

## Context

WP-456 established a static 256-command physical transaction ceiling while
retaining dynamic FIFO selection. With the existing generated Rust batch limit
of 128 in-flight items, the 19,220-command full seed formed 341 physical
commits with a mean group of 56.47 and a largest group of 112. Every scheduler
dispatch reported `QueueDrained`; neither the 256-command count ceiling nor the
16 MiB staged-write ceiling was reached. About 2.5 of 4.0 seconds remained in
durable flushes.

The client and server bounds no longer match the accepted physical bound. Rust
and Python allowed at most 128 generated-batch items in flight, generated
TypeScript allowed 32, and the production coordinator admitted only 128
messages despite ADR-0096 requiring admission capacity above one maximum
physical group. This prevents a bounded sweep from determining whether the
writer can use its existing safe headroom.

## Decision

Generated Rust, TypeScript, and Python application batches accept explicit
concurrency from 1 through 128. Zero and 129 are rejected locally before any
item is submitted. The 4,096-input bound and resumable checkpoint rules are
unchanged. TypeScript therefore rises from 32 to the existing Rust/Python
ceiling, but no application surface exposes the rejected 192 or 256 candidates.

Rust maps item concurrency to ordinary public transport batches of at most 16
items. At concurrency 128 it may have at most eight transport exchanges in flight.
TypeScript and Python retain bounded worker/semaphore scheduling over the same
ordinary generated command method. No language exposes storage mutation,
collection atomicity, a transaction callback, or a shared idempotency identity.

Production coordinator admission defaults to 512 messages: two maximum
256-command physical groups may wait while one writer unit commits. The
existing retained-byte admission limit remains independent. Application
callers cannot configure the coordinator queue or physical group ceiling.
The existing internal environment override remains saturation-test-only and
does not alter the transaction, byte, or public transport bounds.

A candidate build was measured at requested concurrency 128, 192, and 256. All
three completed without overload or semantic failure, but 192 regressed the
full seed from 3.77 to 5.10 seconds. The 256 candidate finished in 3.97 seconds
and regressed representative unary mutation p50 from 2.08 to 2.80 milliseconds.
Although completion groups fell from 342 at 128 to 252 and 195, serialized
validation/encoding/staging grew enough to erase the flush savings. The
retain-or-revert gate therefore rejects both wider public limits.

Every item retains independent authorization, canonical input, idempotency,
request and commit identity, typed outcome/error, provenance, audit lifecycle,
retry budget, cancellation, progress, checkpoint, acknowledgement, and
uncertainty recovery. The server remains free to form smaller dynamic physical
groups. The oldest-command 200-microsecond deadline is not extended.

## Consequences

- Generated clients now share one 128-item maximum rather than TypeScript
  silently stopping at 32.
- The 512-message coordinator admission queue can retain work arriving during
  one commit without increasing a transport request beyond 16 items.
- Default and maximum application and benchmark concurrency remain 128 until
  evidence justifies a separate bounded adaptive-default decision.
- Unary calls do not traverse generated-batch orchestration and receive no new
  grouping delay.
- Fewer physical commits are not treated as success when total work or unary
  latency regresses; the measured result redirects optimization toward
  validation/encoding/staging CPU rather than more client concurrency.

## Rejected alternatives

- **Make concurrency unbounded or derive it from input length.** This violates
  client memory, transport, response, and recovery bounds.
- **Send 256 commands in one RPC.** The public request remains capped at 16 so
  validation, response, cancellation, and recovery state stay bounded.
- **Retain concurrency 192 or 256 because it forms larger groups.** Both
  candidates reduced commit count but lost on end-to-end seed time, and 256
  materially regressed unary latency.
- **Increase the scheduler wait.** Every observed group was queue-drained; a
  longer unary-visible deadline is not required to test writer feeding.
- **Expose the coordinator capacity to applications.** Server admission and
  retained-byte safety remain deployment-internal invariants.

## Acceptance reference

The maintainer approved proceeding with the bounded writer-feeding work after
reviewing the WP-456 evidence on 2026-08-05. The approval retained the public
16-item transport request, dynamic physical grouping, Immediate durability,
independent command semantics, and fail-closed uncertainty recovery.
