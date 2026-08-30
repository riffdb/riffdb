# ADR-0171: Measurement Stability Binds the Gated Backend

- **Status:** Accepted
- **Obligations:**
  - `OBL-0171-1` The verifier binds the spread rule to the gated backend and
    treats the comparator's spread as published disclosure, in both directions
    and with each direction separately load-bearing: an unstable comparator
    passes carrying its spread and a disclosure marker, a stable comparator is
    not marked disclosed, and an unstable RiffDB fails on a receipt that is
    otherwise self-consistent.
    Proof: `check-wp674-unary-baseline --self-test`
- **Direction approved:** 2026-08-30
- **Exact text accepted:** Yes, 2026-08-30
- **Accepted:** 2026-08-30
- **Acceptance reference:** Maintainer approved the direction in the current
  Claude Code session on the measurement recorded under "Evidence", then
  accepted this record's exact text on being told it awaited that acceptance
- **Decision deadline:** Before WP-674 attempts another unary bank
- **Requires:** ADR-0142, ADR-0143, ADR-0146
- **Amends:** `PERF-008`'s unary evidence-integrity rule
- **Defines or blocks:** WP-674, and through it WP-623, WP-578, and WP-579

## Context

ADR-0142 retired the universal 1.10× unary PostgreSQL ratio after WP-670 and
WP-673 proved it arithmetically unreachable. Unary qualification became fixed
absolute service levels plus a downward-only RiffDB regression bank, and every
safe-application PostgreSQL unary ratio became **published disclosure evidence
rather than a release gate**.

ADR-0143 set the evidence-integrity rule: five counterbalanced generations,
sort the retained values, and treat evidence as invalid when `x4 / x2` exceeds
1.20. ADR-0146 raised each generation from 100 to 1,000 measured operations
after the first attempts proved scheduler-sensitive.

That rule is written "for each backend and statistic". PostgreSQL must run in
every unary generation to produce its mandatory disclosure, so its spread
participates in evidence validity even though its numbers gate nothing.

## Evidence

`release/evidence/wp-674/` has held only `adr0143-failed-attempts-v1.json`
since 2026-08-24. Both attempts read `correctness_clean: true`,
`absolute_service_level_passed: true`, and
`failure: postgres_safe_app_p95_central_three_spread`.

A fresh receipt-mode run on N1 at `35fe502d` under ADR-0146's protocol — five
generations, 20 warmups, 1,000 measured operations, PostgreSQL 18 at `fsync=on`
and `synchronous_commit=on`, correctness clean — gives the central-three spread
per backend:

| scenario | PostgreSQL p95 | RiffDB p95 |
|---|---:|---:|
| board_page_200 / 450 / 50 | 1.004 / 1.030 / 1.054 | 1.003 / 1.028 / 1.020 |
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

RiffDB is stable on all fourteen, between 1.002 and 1.028. PostgreSQL exceeds
1.20 on five. Raising generations to 1,000 operations did not fix the
comparator; it moved which scenarios fail, since `point_get_ticket` failed
under ADR-0143 and now passes.

Detail in `docs/performance/wp-674-comparator-stability.md`. That run is
diagnostic — one host, one profile, no host-validity preflight or postflight —
and is not eligible for `PERF-018` qualification.

## Decision

The unary evidence-integrity spread rule binds the **gated backend only**.

RiffDB's qualified p50 and p95 must satisfy `x4 / x2 <= 1.20` for every
scenario on every profile, exactly as today. PostgreSQL's spread is computed,
retained, and published with its absolute values and ratios, and does not
invalidate the receipt.

Proposed SPEC text, replacing the sentence that applies the rule to both
backends:

> For each statistic, sort the five retained RiffDB generation values as
> `x1 <= x2 <= x3 <= x4 <= x5`; the qualified statistic is `x3`, and evidence
> is invalid when `x4 / x2` exceeds 1.20. The same five-value summary and
> spread MUST be computed and published for safe-application PostgreSQL, and a
> comparator spread above 1.20 MUST be disclosed in the receipt, but it does
> not invalidate the evidence. Host validity, correctness, and comparator-input
> drift remain invalidating for either backend.

Nothing else changes. Both extremes remain mandatory retained evidence, no
value may be deleted or retried on performance, every absolute service level
and every frozen RiffDB regression ceiling is unchanged, and every PostgreSQL
ratio remains mandatory published disclosure.

## Options Considered

**Give the comparator a looser bound.** Keep the rule on both backends with a
wider PostgreSQL limit. Rejected: the second threshold has no principled
source, and choosing it from the data that failed is fitting the bound to the
observation. It also keeps a comparator's variance able to block a release,
merely at a different value.

**Make the comparator quieter and keep one rule.** Pinned CPU, dedicated
disk, longer warmup. This is the most faithful reading of ADR-0142/ADR-0146 as
written and was seriously considered. Rejected as the primary route because it
spends host-tuning effort for no product improvement, because a shared-tenant
cloud disk may not reach 1.20 at all — E2's earlier spread was 1.50 — and
because that would be learned only after the effort. It remains available as an
additional measure and this record does not forbid it.

**Leave the rule and keep failing.** Rejected. It has produced no banked
baseline since 2026-08-24 while RiffDB itself measured stable throughout, and
it blocks WP-623, WP-578, and WP-579 behind a property RiffDB cannot influence.

## Consequences

WP-674 can attempt a bank whose validity depends on the artifact under test
rather than on the comparator's variance.

The comparator's instability becomes visible rather than fatal: a receipt
carrying a 1.46 PostgreSQL spread says so, and a reader can weigh the published
ratio accordingly. That is a more honest presentation than discarding the whole
receipt, which published nothing at all.

The risk is that a reader treats a ratio computed against an unstable
comparator as firm. The disclosure requirement above is what answers that, and
the receipt must carry the spread beside the ratio rather than in a separate
document.

## Compatibility

None. No durable format, wire surface, or generated artifact changes. Prior
receipts remain readable; the two failed attempts remain retained evidence and
are not reclassified as passes.

## Security

None. Measurement governance carries no authority, credential, or data path.

## Standing Design Tests

- A receipt whose RiffDB spread exceeds 1.20 is invalid.
- A receipt whose PostgreSQL spread exceeds 1.20 is valid and carries the
  spread and a disclosure marker.
- A receipt that omits PostgreSQL values, ratios, or spread is invalid.
- Host-validity, correctness, and comparator-input drift still invalidate.

## Testing

`scripts/check-wp674-unary-baseline` owns the rule and has a self-test. It has
gained arms for both directions above: an unstable comparator that passes with
disclosure and retains its spread, a stable comparator that is not marked as
disclosed, and an unstable RiffDB that fails.

The two failing directions are separately load-bearing, checked by drifting the
implementation and confirming each arm speaks on its own. The pre-existing
`spread` arm mutates only the summary, so it can also fail on the
ratio-derivation check and pass vacuously when the stability rule is removed;
the new `riffdb_spread` arm keeps the whole receipt self-consistent so the
stability rule is the only thing left to reject it.

`method.stability_rule_binds` is recorded in the baseline receipt, so a
candidate measured under the old both-backends rule fails the existing
method-drift check rather than being silently compared against a baseline that
meant something different.

## Requirements and Work Packages

Amends `PERF-008` and `PERF-018`. WP-674 implements the split rule in the
verifier and its self-test, discharged as `OBL-0171-1`. What remains for WP-674
is the qualification run itself.

## Decision Deadline

Before WP-674 attempts another unary bank. Attempting one under the current
rule would consume a multi-hour three-profile run to produce the same
comparator-spread refusal.
