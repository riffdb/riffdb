# ADR-0142: Portable Unary Service Levels and Comparator Evidence

- **Status:** Accepted
- **Direction approved:** 2026-08-23 (maintainer)
- **Exact text accepted:** Yes, 2026-08-23 (maintainer)
- **Decision deadline:** Before WP-674 banks the alpha unary baseline or WP-623
  starts final performance qualification
- **Requires:** ADR-0056, ADR-0059, ADR-0123, ADR-0132, ADR-0141
- **Amends:** ADR-0059's checked-profile parity gate, ADR-0123 Decision 1,
  ADR-0132's release gates, ADR-0141 Decisions 6 and 7, and `PERF-008`

The maintainer approved the direction and accepted this exact text on
2026-08-23 after WP-673 closed the API-neutral service-floor ledgers.

## Context

The alpha performance rules currently require every representative unary
scenario to complete within 1.10 times same-run safe-application PostgreSQL on
the inventoried profiles. That rule was useful while a removable transport,
storage, query, or coordinator mechanism could plausibly explain the gap. It
forced rejected candidates to stop instead of weakening safety or declaring a
partial improvement sufficient.

WP-670 removed the last private transport candidate after the complete unary
matrix still missed the rule. WP-673 then measured all seven ordinary reads and
four commands in isolated process generations on both cloud hosts. Its closed
ledgers prove that the remaining rule is not a bounded optimization target:

- every failing read would require removing between 1.74 and 8.06 times the
  complete named-query execution stage;
- the two remaining E2 command misses would require removing 99.57 and 99.85
  percent of the largest validation, encoding, and staging family; and
- these families contain current authorization, bounded plan execution,
  deterministic validation, atomic command construction, durability, and
  response-release work that RiffDB exists to guarantee.

The comparator is still valuable. It exposes product cost, catches workload or
implementation drift, and prevents marketing from hiding an unfavorable
result. It is not, however, a valid universal release threshold when the
denominator is a different system with a materially smaller safety contract and
the measured RiffDB floor already exceeds the budget.

The specification also carries two overlapping historical rules. `PERF-005`
still says the TicketDesk seed and representative unary mutation p50 must be
within twice PostgreSQL, while the later `PERF-008` amendment makes seed a
receipted 5.0-times ceiling and requires every unary scenario to be within
1.10 times PostgreSQL. One requirement must own release performance so an
artifact cannot simultaneously pass and fail different revisions of the same
product decision.

This decision changes only release qualification. It does not reduce any
correctness, authorization, boundedness, durability, freshness, uncertainty,
recovery, or application-interface guarantee.

## Proposed Decision

### 1. Mixed-load comparison remains a release gate

The accepted concurrent real-world gates remain exact on every `PERF-018`
release profile:

- at 32 clients, public interactive mixed-workload throughput is at least 0.90
  times same-run safe-application PostgreSQL;
- at 32 clients, public interactive mixed-workload p95 is at most 1.25 times
  same-run safe-application PostgreSQL; and
- the full public TicketDesk seed is receipted and remains below the existing
  5.0-times same-run PostgreSQL regression ceiling.

Correctness reconciliation, durability, public gRPC shape, workload weights,
dataset, isolation, percentile calculation, host validity, repetition length,
and all other `PERF-018` freezes remain unchanged. Minimal PostgreSQL remains a
reported floor, never the release peer.

### 2. Unary release qualification uses explicit cloud service levels

The universal unary PostgreSQL ratio is replaced by the following absolute
alpha qualification levels on each inventoried N1 and E2 cloud profile:

| Frozen scenario class | Included scenarios | p50 ceiling | p95 ceiling |
|---|---|---:|---:|
| Ordinary named reads | `point_get_ticket`, `point_get_user`, `list_tickets_by_project_status`, `list_open_tickets_for_assignee`, `list_comments_for_ticket`, `list_project_members`, `ticket_detail_page` | 3 ms | 6 ms |
| Wide bounded named reads | `board_page_50`, `board_page_200`, `board_page_450` | 6 ms | 12 ms |
| Compiled commands | `create_comment`, `close_ticket_with_comment`, `swap_member_roles`, `open_ticket_with_labels` | 10 ms | 15 ms |

These are release-qualification thresholds for the recorded alpha hardware and
workload, not a hosted-service SLA or a promise that arbitrary future queries
fit the same latency class. The classes are frozen by scenario identity before
measurement. A new representative operation must be assigned to an existing
class, or introduce a separately accepted class, before its result is known;
benchmark code cannot reclassify a miss after measurement.

Unary evidence consists of three counterbalanced, independent process
generations per backend, scenario, and host. RiffDB is restarted after common
setup and each generation then executes 20 same-scenario warmups followed by
100 measured operations over the full frozen dataset. The qualified p50 and
p95 are the medians of the three per-generation statistics. Evidence is invalid
when the largest and smallest per-generation statistic differ by more than 20
percent, the host-validity inventory fails, correctness differs, or any
semantic comparator input drifts. One scenario therefore cannot borrow
process-local state from another timed scenario, while the qualified statistic
still measures a warmed application process rather than a startup probe.

The high-IPC workstation remains mandatory for regression evidence and the
full mixed matrix. The absolute cloud ceilings do not turn a workstation
regression into a pass.

### 3. A byte-exact RiffDB baseline prevents self-regression

WP-674 banks one current-production baseline using the method in Decision 2.
The receipt binds:

- exact source, release binary, generated application lock, contract/module/
  plan identities, benchmark and report-schema versions;
- complete N1, E2, and workstation inventories;
- every per-generation result and merged qualified statistic;
- safe-application and minimal PostgreSQL configuration and results; and
- value-free report digests and host-validity observations.

After that bank, a release candidate's qualified unary p50 and p95 for every
scenario on every profile must be no more than 1.10 times the corresponding
frozen RiffDB baseline. It must also meet the absolute N1/E2 ceilings. Faster
operations cannot compensate for a regression in another operation or tail.

The baseline cannot be refreshed because a candidate is slower. A fully valid
accepted release may ratchet an individual scenario/profile/statistic baseline
downward; the registry retains the exact source receipt for every low-water
mark and never synthesizes an unreceipted value. Moving any low-water mark
upward requires a separately accepted decision naming the semantic, workload,
compiler, transport, durability, dataset, or hardware-profile change and
overlapping old/new evidence. A replacement cloud machine may satisfy the same
profile only when its inventoried CPU, vCPU count, storage class, operating
system, and benchmark topology satisfy the frozen profile rules; otherwise it
is a new profile and requires review.

### 4. PostgreSQL unary ratios remain mandatory published evidence

Every unary qualification still runs safe-application PostgreSQL in the same
counterbalanced generation and reports, for every scenario and host:

- both systems' p50 and p95;
- the RiffDB/PostgreSQL ratios;
- absolute latency and encoded request/response size;
- correctness and semantic-obligation reconciliation; and
- any invalid or unstable repetition without deletion.

Those unary ratios are diagnostic and disclosure evidence, not release gates.
They may motivate later optimization work, but may not waive an absolute or
RiffDB-regression miss. Reports and public claims must not say RiffDB has
PostgreSQL unary parity when the table says otherwise.

### 5. One requirement owns performance qualification

Upon exact acceptance, `PERF-008` becomes the sole owner of the alpha's
scenario performance thresholds. `PERF-005` keeps all compiler, partition,
conflict, revalidation, and grouping semantics, but its historical
checked-profile seed/unary parity paragraph is replaced by a normative
reference to `PERF-008`.

ADR-0123 Decision 1 is amended so “the existing `PERF-008` ratios” means the
mixed/seed comparison gates plus the unary absolute and RiffDB-regression gates
in this decision. ADR-0132's unary comparator gate is replaced by the same
unary qualification rule.

ADR-0141 remains the authoritative history for the rejected private transport
and its removal. Its Decisions 6 and 7, activation list, and Option 4 rejection
are superseded only as future release criteria. This decision does not revive
WP-670's protocol identity, authorize WP-671 or WP-672, add a transport, or
change unary gRPC as the release-selected application path.

WP-623 remains the final full performance qualification package. It may begin
only after WP-674 banks the accepted unary baseline and updates the release
gate implementation.

## Options Considered

1. **Keep universal 1.10-times PostgreSQL:** rejected by the closed WP-673
   arithmetic. Passing requires deleting more than all movable read work or
   nearly all required command safety work.
2. **Remove PostgreSQL unary comparison entirely:** rejected. The comparator is
   useful disclosure, diagnosis, and product-cost evidence even when it is not
   the release threshold.
3. **Use only generous absolute ceilings:** rejected. An absolute ceiling alone
   permits gradual product regression while staying below it.
4. **Use only a RiffDB self-regression ceiling:** rejected. It can freeze a
   product that is consistently too slow for an application.
5. **Absolute cloud SLOs plus a frozen RiffDB regression bank, with PostgreSQL
   disclosed and mixed-load comparison retained:** proposed. It measures
   customer-visible usability, prevents backsliding, and preserves honest
   cross-product evidence without requiring removal of RiffDB's guarantees.

## Consequences

- The alpha can qualify only when unary operations are both practically fast
  on ordinary cloud hardware and no slower than the banked RiffDB artifact.
- PostgreSQL may remain faster for some unary operations; that result remains
  visible in every release receipt.
- Mixed throughput and tail latency remain comparative release gates because
  they measure the real application workload rather than one differing
  primitive.
- The baseline bank becomes governed release evidence and must be reproduced
  when an accepted change legitimately rotates its identity.
- WP-670's transport experiment remains removed. No implementation is
  resurrected by changing the product rule.
- Post-alpha work may still pursue PostgreSQL parity, conflict-domain-parallel
  batch apply, lower unary floors, and better wide-result scaling, but those are
  no longer allowed to churn the alpha indefinitely without a measured
  customer-visible regression or SLO miss.

## Compatibility

This decision changes no public gRPC, MCP, CLI, generated SDK, contract grammar,
RiffQL, executable IR, bundle/module/plan hash, capability, durable record,
journal, backup, export, changelog, replication, transport, or stored-data
format. It changes only performance qualification, benchmark report metadata,
and release governance.

Existing performance receipts remain valid historical evidence under the rule
that produced them. They are not relabeled as passes. The new unary baseline is
banked from a fresh exact artifact after acceptance; WP-673's short diagnostic
receipts cannot be promoted into that baseline.

## Security

No application principal gains a knob, bypass, alternate transaction path, or
ability to select a benchmark class. Authorization, row and field policy,
freshness, response release, idempotency, atomicity, durability, uncertainty,
and recovery remain unchanged and must execute during every measured operation.
Evidence is value-free, credential-free, fixed-cardinality, and bound to exact
public operation identities.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** the decision adds no application
  interface. A benchmark or release operator cannot disable or weaken a data
  guarantee to pass a latency ceiling; any semantic or topology drift
  invalidates the evidence.
- **Scale:** the qualification matrix is finite and bounded. It assumes no
  full-state cache, co-located authority, or full-data rewrite. The wide-read
  class remains compiler-bounded to the accepted 500-row response ceiling and
  does not imply that latency is independent of result size.

## Testing

- A report-schema test freezes scenario membership, p50/p95 arithmetic,
  three-generation median selection, the 20-percent stability refusal, and
  absolute/regression gate evaluation.
- Negative fixtures reject a missing host/backend/scenario/generation,
  post-result reclassification, changed workload or dataset, invalid host,
  correctness mismatch, and baseline identity drift.
- WP-674 runs and receipts the exact unary bank on N1, E2, and the workstation,
  then runs the unchanged mixed/seed controls.
- `check-requirement-coverage`, workspace policy, generated artifacts, and the
  handbook gate run with the accepted specification amendment.

## Requirements and Work Packages

- **Requirements:** `PERF-005`, `PERF-008`, `PERF-018`, `END-007`, `END-008`
- **Defines or blocks:** `WP-674`, `WP-623`, `WP-579`
- **Final evidence:** `WP-674` banks the unary baseline; `WP-623` qualifies the
  unchanged release artifact

## Decision Deadline

Exact acceptance is required before `SPEC.md`, benchmark assertions, WP-623,
or the release evidence schema adopts this rule. Direction approval alone does
not authorize relabeling any prior miss.
