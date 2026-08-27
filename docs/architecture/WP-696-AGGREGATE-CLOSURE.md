# WP-696 exact aggregate closure

Status: complete evidence for ADR-0152.

RiffDB's exact aggregate registry contains `count`, `exact_count`, `sum`,
`min`, `max`, `count_present`, `count_distinct`,
`count_distinct_present`, `mean`, `any`, and `all`. Applications select these
only in compiled named RiffQL; request values cannot choose a function,
provider, population class, approximation, policy, epoch, bound, or fallback.

## Evidence matrix

| Obligation | Automated evidence |
|---|---|
| Empty, one-row, `NoValue`, duplicates, exact mean, Boolean identities | `exact_aggregate_core_freezes_novalue_empty_and_mean_state` in the authoritative executor and `acceptance_query_range_sort_limit_aggregates_group_by_budgets` in columnar |
| Maximum and overflow refusal | `distinct_budget_exhaustion_withholds_the_complete_aggregate`, aggregate arithmetic-overflow tests, group-cardinality clamps, and response-byte withholding tests |
| Grouped canonical order | operational aggregate tests plus columnar grouped-result and service projected-read acceptance |
| Policy before contribution | `protected_snapshot_admission_precedes_scan_budget_and_aggregate`, service pre-shape admission architecture, and exact-result protected-slot tests |
| Provider subsets | `aggregate_provider_capabilities` plus the real columnar and exact-text descriptor tests; columnar advertises the bounded exact core, exact text advertises only `exact_count`, and vector advertises none |
| One provider epoch | result-set epoch intersection tests, exact page/count execution tests, and projected aggregate frontier/token acceptance |
| Rebuild, compaction, recovery, retention | columnar randomized history, compaction, checkpoint/replay, crash-child, exact-provider rebuild/recovery, and memory/redb workspace recovery suites |
| Compatibility | RiffQL V8, query-IR V11, and query-module V11 old/current tests; provider V1 bytes and state layouts remain unchanged by the transient capability subset |
| Public result carriage | query-module generation for Rust, Go, TypeScript, and Python; structural `ExactMeanV1 { total, count }` gRPC and MCP schema tests; remote driver conformance |
| Generic application acceptance | the framework-neutral operational-conformance application and shared remote driver corpus compile and execute generated named operations without client aggregate logic |

The reference, ordinary, columnar, and exact-result planes retain different
population and scale contracts. Ordinary and grouped folds operate only on an
earlier compiler-bounded source. The indexed exact-result provider supplies
`exact_count` for the complete admitted population. A provider cannot fold a
page, walk cursors, apply policy after contribution, mix epochs, materialize an
unbounded population, or return an approximation under an exact identity.

## Merge, bounds, and refusal

Counts and mean counts use checked unsigned addition; sums and mean totals use
checked widened exact arithmetic; extrema use the frozen typed comparator;
`any` and `all` use their exact Boolean identities; distinct counts use a
bounded canonical typed set. Empty and partitioned merge schedules therefore
produce the same result as a direct fold or return a typed whole-result
refusal. No partial aggregate, group, distinct member, sum, extreme, or hidden
cardinality is released when a row, group, distinct, state-byte, arithmetic,
scan, projected-cell, or output budget is exhausted.

Descriptor validation is catalog-generation scoped and epoch negotiation is
opened-result-set scoped. Neither is reconstructed per contribution, group,
partial merge, measure, page item, or page. Fresh authentication,
authorization, capability revision, row policy, field policy, revocation, and
final release checks remain request scoped.

## Security and inference boundary

Partition-scoped provider state is primary. Compiler-proved policy
subpartitions are secondary; bounded row admission may only refine a finite
candidate set before any count, facet, aggregate, order, ordinal, cursor, or
work-class decision. Denied rows and other organizations cannot affect values,
group presence, provider statistics, bounds, diagnostics, or released timing
class. Secret aggregate inputs and outputs require the same compiled field
authority as ordinary results, and safe diagnostics contain no business
values, partial states, group keys, or tenant cardinalities.

## Deliberately unavailable

Variance, deviation, covariance, percentile, median, histogram, approximate
distinct, top-k, ordered selectors, collection-producing folds, and user-
defined aggregates remain unavailable. There is no target-language callback,
floating-point business aggregate, external-framework runtime branch,
cross-provider bridge, bitmap/row-ID transfer, or unsafe scan fallback. A real
consumer and a separately accepted ADR must define numerical or quality
semantics, bounds, policy, epoch, and durable state before any such family can
become executable.

