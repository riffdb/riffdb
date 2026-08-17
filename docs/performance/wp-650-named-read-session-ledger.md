# WP-650 exact named-read session ledger

Date: 2026-08-17

Status: complete without production activation. The existing bounded session
does not reduce exact single-client named-read latency on either cloud CPU
family. No authentication, authorization, service-dispatch, generated-client,
or `PERF-018` behavior changed.

## Decision question

WP-649 left small named reads above the same-run safe-application PostgreSQL
comparator even though authoritative entity lookup was already cheap. WP-650
asked whether two remaining server stages justified a safety-sensitive change:

1. authenticate immutable credential presentation once per bounded session,
   while retaining current capability, expiry, operation, row/field policy,
   and response-release checks for every operation; and
2. remove or replace the service task handoff for compiler-bounded named reads
   without weakening cancellation, panic containment, audit lifecycle, or
   admission.

The reject-first threshold was at least a 20% exact bounded-session `GetTicket`
mean improvement on both N1 and E2. Missing it forbids an ADR-0127 amendment or
production implementation.

## Method

The diagnostic uses production code at `a9c4cefb` (the current head differs
only by WP-649 evidence/closure commits), the full 19,220-command TicketDesk
dataset, one client, 64 warmup operations, and 1,000 measured generated
`GetTicket` operations. Unary
and bounded-session cells run back-to-back against fresh measurement-process
generations. Both use the same exact application lock, credential, persistent
HTTP/2 channel, named query, parameters, result decoder, and synchronous public
client shape. The bounded session adds only the accepted ADR-0127 application
stream; session establishment is outside the measured operations.

The diagnostic is not `PERF-018` evidence and cannot change the frozen release
comparator. PostgreSQL twins were measured in the immediately preceding exact
unary run on each otherwise idle host. The back-to-back unary/session result,
not the PostgreSQL twin, decides this candidate.

## Current-head safe-application reference

| Host | Safe PG mean | RiffDB unary mean | RiffDB/PG mean | Safe PG p50 | RiffDB p50 |
|---|---:|---:|---:|---:|---:|
| N1 | 255 us | 581 us | 2.27x | 254 us | 590 us |
| E2 | 435 us | 884 us | 2.03x | 328 us | 885 us |

The E2 PostgreSQL distribution has a noisy tail (435 us mean versus 328 us
p50), so it is retained as a target reference rather than used to select the
session candidate.

## Exact unary versus bounded session

| Host | Unary mean | Session mean | Change | Unary p50 | Session p50 | Decision |
|---|---:|---:|---:|---:|---:|---|
| N1 | 557 us | 572 us | +2.7% | 557 us | 590 us | reject |
| E2 | 734 us | 848 us | +15.5% | 721 us | 819 us | reject |

Positive change is slower. The session misses the predeclared improvement
threshold on both hosts and regresses beyond the five-percent no-regression
limit on E2.

The corresponding fixed-cardinality server-stage sums are 120 us unary versus
109 us session on N1 and 138 us unary versus 162 us session on E2. The session
still deliberately invokes the ordinary unary application handlers, so it does
not bypass per-operation authentication, service admission, authorization, or
task supervision.

## Closed stage and Amdahl ledger

The independent current-head unary run attributes the following mean server
costs:

| Stage family | N1 | E2 | Perfect-removal gain |
|---|---:|---:|---:|
| credential authentication | 9 us | 10 us | 1.5% / 1.1% |
| service spawn/dispatch | 15 us | 22 us | 2.6% / 2.5% |
| authentication + dispatch | 24 us | 32 us | 4.1% / 3.6% |
| every measured server stage | 123 us | 175 us | 21.2% / 19.8% |
| customer-paid remainder outside stage clocks | 458 us | 709 us | 78.8% / 80.2% |

Even perfect deletion of both proposed stages cannot meet the 20% candidate
threshold, cannot bring either cloud result near 1.10x PostgreSQL, and would
not explain the measured residual. The accepted session itself confirms the
bound by making no material N1 improvement and regressing E2.

The remainder includes client/session protocol scheduling, HTTP/2/tonic work
outside handler clocks, process wakeups, and measurement boundary time. The
benchmark bridge is separately paired at roughly 4--7 us and is a measurement
artifact, not a product opportunity.

## Safety decision

No ADR amendment is justified. RiffDB continues to authenticate every session
operation and to perform all current authorization safe points. It does not
cache a positive allow decision or give a transport adapter storage, plan,
policy, audit, or result authority.

Inlining service execution is also rejected without production editing. The
current spawned service job is part of cancellation, panic, and audit-lifecycle
containment; removing it for a theoretical 15--22 us maximum would trade a
safety boundary for less than a five-percent end-to-end ceiling.

Unary remains the generated-client default. The bounded session remains an
accepted optional protocol and retains the c32 throughput/tail evidence from
WP-649, but it is not activated as `PERF-018` release evidence.

## Workstation attempt and reliability note

Two current-head full-scale workstation attempts completed the 19,220-command
seed and then failed a later measurement-process restart with the typed
`riffdbd process failed: ready timeout` outcome. Neither partial attempt wrote
a report, and no workstation number is fabricated. Both cloud profiles reached
clean shutdown and independently reject the candidate, so the failed local
column cannot reverse the decision. The repeated restart failure remains a
harness/reliability finding rather than a performance result.

## Receipts

Exact unary plus safe-application PostgreSQL:

- N1: `5bd1fd30131d1775b8751bc3e5061b21620ccfa6fb959401d74eb20bff023929`
- E2: `20754e00529dcb6ccd5138b7311fd472efb4e523083b0ed482441ec095cf17c3`

Back-to-back unary plus bounded session:

- N1: `d08fc70a647a2cfd8b7cf982e9965d8b6c19fc37ca4d45f4b855c0bb1f422af9`
- E2: `d042440dbee80c190cf6efea7bf3938f3211e27f311adb153859704997e1a831`

Raw receipts are retained under `/home/kevin/tmp/` on the controlling host and
`/home/kevin/tmp/` on each cloud host. They contain no credential or application
values.

## Follow-up ownership

This ledger rejects authentication and dispatch work; it does not relax the
unary performance gate. The next measured candidates are:

- result assembly and ordinal windowing for `BoardPage450`, owned by the
  accepted ADR-0130/ADR-0131 result-set campaign; and
- a future exact attribution of the customer-paid protocol/scheduling
  remainder, only if it can predeclare a material public gain without changing
  authorization or the frozen comparator.

No entity-cache, compression, batching-window, lazy-decode, coordinator, or
seed-parity work is reopened by this result.
