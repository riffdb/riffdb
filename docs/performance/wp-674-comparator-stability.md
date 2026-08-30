# WP-674: the comparator is what fails, not RiffDB

`release/evidence/wp-674/` holds one artifact: `adr0143-failed-attempts-v1.json`,
two attempts that both read

    correctness_clean: true
    absolute_service_level_passed: true
    failure: "postgres_safe_app_p95_central_three_spread"

RiffDB passed. The PostgreSQL comparator's p95 was too unstable across the five
retained generations to constitute valid evidence. ADR-0146 responded by
raising each generation from 100 to 1,000 measured operations, explicitly to
address scheduler sensitivity.

This records whether that worked.

## Run

N1 (`instance-20260815-150253`, 8 vCPU, idle), current `main` at `35fe502d`,
release build, `RIFFDB_APP_BASELINE_WP674_RECEIPT=1`, five generations, 20
warmups, 1,000 measured operations each, PostgreSQL 18 at `fsync=on`,
`synchronous_commit=on`, `full_page_writes=on`. Correctness reconciliation
clean.

This is one host and one run. It is not a qualification: no host-validity
preflight or postflight, one profile rather than three, and no manifest was
assembled. It is a targeted answer to one question.

## Result

ADR-0146's rule: sort five generation values, evidence is invalid when
`x4 / x2 > 1.20`, applied *for each backend and statistic*.

| scenario | PostgreSQL p95 x4/x2 | RiffDB p95 x4/x2 |
|---|---:|---:|
| board_page_200 | 1.004 | 1.003 |
| board_page_450 | 1.030 | 1.028 |
| board_page_50 | 1.054 | 1.020 |
| close_ticket_with_comment | 1.151 | 1.008 |
| create_comment | 1.083 | 1.014 |
| list_comments_for_ticket | 1.160 | 1.002 |
| **list_open_tickets_for_assignee** | **1.294** | 1.017 |
| **list_project_members** | **1.433** | 1.007 |
| **list_tickets_by_project_status** | **1.426** | 1.009 |
| open_ticket_with_labels | 1.169 | 1.007 |
| point_get_ticket | 1.081 | 1.003 |
| **point_get_user** | **1.398** | 1.013 |
| swap_member_roles | 1.144 | 1.007 |
| **ticket_detail_page** | **1.459** | 1.006 |

**RiffDB is stable on every scenario** — 1.002 to 1.028, effectively
noise-free across 1,000-operation generations.

**PostgreSQL exceeds the limit on five of fourteen.** Raising generations to
1,000 operations did not fix the comparator. It moved which scenarios fail:
`point_get_ticket`, which failed under ADR-0143, now passes at 1.081, while
five list and detail scenarios do not.

## What this means for the gate

ADR-0142 made unary PostgreSQL ratios **disclosure evidence rather than release
gates**. But the evidence-integrity rule is written "for each backend", and
PostgreSQL must still run in every generation, so its instability invalidates
the whole receipt.

The result is that RiffDB's release qualification is gated on the measurement
stability of a comparator whose numbers are explicitly not gates, on hardware
RiffDB does not control. RiffDB can be perfectly stable — it is, at 1.002 to
1.028 — and still be unable to bank a baseline.

That is a gate-design problem, not a performance problem, and it is why the
WP-674 evidence directory has held only failed attempts since 2026-08-24.

## Options, none of which this note chooses

1. **Apply the stability rule only to the gated backend.** RiffDB's values gate
   the release; PostgreSQL's are disclosure. Publish the comparator's observed
   spread as part of the disclosure rather than as a validity precondition.
2. **Give the comparator its own looser stability bound**, on the argument that
   a shared-tenant cloud disk cannot hold a p95 as still as a local engine can.
3. **Make the comparator quieter** — pinned CPU, dedicated disk, longer warmup —
   and keep one rule for both. This is the most faithful to the current record
   and the most likely to keep failing on E2, whose earlier spread was 1.50.

Each is an amendment to ADR-0142/ADR-0146 rather than an implementation
choice, so each needs acceptance before a bank can be attempted under it.

## Receipt

Report SHA-256 `7b1fdacd7916ea5db0ab12605e88f70cf93b389063e7504bc0d50616b40539be`,
retained on N1 at `/home/user/tmp/wp674-stability.json`. Diagnostic only --
it carries no host-validity preflight or postflight and is not eligible for
PERF-018 qualification.

## What is not in question

The artifact meets its absolute service levels. Both banked failures recorded
`absolute_service_level_passed: true`, and nothing measured here contradicts
that. The blocker is evidence validity, not RiffDB latency.
