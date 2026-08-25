# ADR-0152: Closed Exact Aggregate Function Families Across Query Providers

- **Status:** Accepted
- **Direction approved:** Yes
- **Exact text accepted:** Yes, 2026-08-25
- **Accepted:** 2026-08-25
- **Acceptance reference:** Maintainer exact-text acceptance in the current
  Codex session for commit `a9093c56`
- **Decision deadline:** Before WP-693 adds an aggregate function, descriptor,
  result value, or provider capability
- **Requires:** ADR-0038, ADR-0051, ADR-0053, ADR-0055, ADR-0086, ADR-0087,
  ADR-0108, ADR-0111, ADR-0124, ADR-0129, ADR-0130, ADR-0131, and ADR-0150
- **Amends if accepted:** ADR-0108's initial `count`/`sum`/`min`/`max` registry
  and ADR-0130's whole-result measure vocabulary
- **Defines or blocks:** WP-693 through WP-696

This record is authoritative for WP-693 through WP-696.

## Context

RiffQL already supports multiple `count()`, `sum(field)`, `min(field)`, and
`max(field)` measures over an earlier bounded collection plus bounded group-by.
The columnar engine implements the same four folds, and the exact-result
provider adds `exact_count` over the complete admitted population. These are a
sound base but not a sufficient general-purpose aggregate collection: common
applications also need non-null counts, distinct cardinality, averages, and
Boolean folds.

Adding each function independently would repeat the access-path problem in
ADR-0150. Parser tags, query IR, reference evaluation, columnar execution,
whole-result provider descriptors, result carriage, generated clients, empty-
set behavior, null semantics, arithmetic, policy, and cost could drift. It
would also be easy to call a fold over one bounded `take` an exact population
measure, or to let a projection advertise a function whose state cannot serve
the selected epoch and policy partition.

The execution planes have deliberately different scale contracts:

- an ordinary operational aggregate folds only an already bounded collection;
- a projection-backed measure may cover a complete indexed population at one
  ADR-0130 provider epoch; and
- grouped columnar analytics may scan a compiler-bounded partition and produce
  a separately bounded number of groups.

They need one semantic function registry and result algebra, not one universal
executor. Functions that require approximation, large retained sketches, or
materialized value collections must not enter durable provider formats merely
because they are familiar SQL names.

## Proposed Decision

### 1. Freeze one compiler-owned aggregate capability registry

The compiler owns a closed versioned registry. Each function descriptor names:

- its stable semantic identity and source spelling;
- input arity and accepted exact field types;
- whether `NoValue` participates, is excluded, or makes the input invalid;
- empty-set result;
- exact result schema and arithmetic/overflow rules;
- mergeable partial-state schema, when one exists;
- ordinary bounded-fold, grouped-fold, and whole-result-provider eligibility;
- maximum input rows, distinct values, groups, state bytes, output bytes, and
  diagnostic work; and
- policy/inference classification.

The descriptor is compiler vocabulary, never request input. Applications may
invoke only functions and fields written in a compiled named query. They cannot
supply a function name, field, precision mode, approximation flag, sketch,
grouping expression, provider, or fallback at runtime.

The canonical semantic registry is shared by the RiffQL resolver, exact
reference evaluator, operational runtime, columnar provider adapter, projected
wire conversion, and generated SDK schemas. An executor advertises a checked
subset; the union of provider capabilities is not a requirement that every
provider implement every function.

### 2. Preserve the existing four functions exactly

Existing functions retain their accepted meanings and bytes:

- `count()` returns the exact number of input rows, including rows containing
  optional `NoValue` fields, and returns zero for an empty input;
- `sum(field)` accepts the existing exact integer/decimal inputs, uses checked
  widened accumulation and the existing Decimal carriage, returns additive
  zero for empty input, and retains the explicit money/currency rule;
- `min(field)` and `max(field)` use the field's frozen typed comparison,
  include `NoValue` as the existing real comparable state for optional fields,
  and return absence only when the complete input is empty; and
- grouped output retains canonical encoded group-key order and the requested-
  limit group-cardinality clamp.

`count()` over an ordinary source continues to count only that earlier bounded
collection. `exact_count` remains ADR-0131's complete admitted population
measure. Neither spelling may be substituted for the other.

### 3. Add a practical exact core without silent null or rounding rules

The first expansion adds these closed functions:

| Function | Meaning | Empty input | Initial execution eligibility |
|---|---|---|---|
| `count_present(field)` | rows whose field is not `NoValue` | `0` | bounded/grouped/provider |
| `count_distinct(field)` | distinct canonical typed values, with `NoValue` one value | `0` | bounded distinct cap or exact provider |
| `count_distinct_present(field)` | distinct canonical typed non-`NoValue` values | `0` | bounded distinct cap or exact provider |
| `mean(field)` | exact widened sum plus contributing count | `{ total: 0, count: 0 }` | bounded/grouped/provider |
| `any(field)` | Boolean disjunction | `false` | required `bool` only initially |
| `all(field)` | Boolean conjunction | `true` | required `bool` only initially |

`NoValue` is the single missing/explicit-null order class already accepted by
ADR-0145. The explicit function names avoid importing SQL's dialect-dependent
`count(field)` or `count(distinct field)` null behavior. Distinct equality is
canonical typed equality under the field's declared profile; it is not display
string equality, collation, token equality, or approximate cardinality.

`mean(field)` returns a generated exact record `ExactMeanV1 { total, count }`.
`total` has exactly the same checked Decimal/currency semantics as `sum(field)`
and `count` is the number of contributing rows. Initially the input field must
be required and sum-compatible, so `count` equals the source row count. This
representation is exact, associative, mergeable, and makes empty input
unambiguous. It deliberately does not choose a hidden division scale or
rounding mode. A future scalar `average` may be added only with compiler-fixed
precision, scale, rounding, overflow, generated-type, and provider-state
semantics; it cannot silently reinterpret `mean`.

`any` and `all` initially accept required Boolean fields only. Optional
three-valued Boolean folds require a separately accepted truth table rather
than silently skipping or coercing `NoValue`.

### 4. Separate bounded folds from whole-population measures

Every aggregate plan names its population contract:

1. **Bounded source fold:** folds the exact rows in an earlier compiler-bounded
   collection after its accepted predicate and policy. Work is charged by that
   collection's maximum even when fewer rows arrive.
2. **Bounded grouped fold:** folds the same bounded population into at most the
   compiler-fixed group count, distinct-value count, state bytes, and output.
3. **Exact whole-result measure:** folds the complete predicate- and policy-
   admitted population at one ADR-0130 provider epoch without page/window
   truncation. The selected provider must advertise the exact function and
   prove its maintained state or complete bounded execution.

A descriptor or source spelling cannot change population class at runtime.
Whole-result measures, facets, page rows, exact total, and ordinal window from
one query share one provider epoch and policy partition. A provider cannot
answer a whole-result function by folding a page, walking cursors, applying
policy afterward, materializing an unbounded population, or returning an
approximate sketch under an exact function identity.

### 5. Make partial-state composition explicit and bounded

Exact providers may maintain or merge only the registry's checked partial
states:

- counts add with checked unsigned arithmetic;
- sums and mean totals add with checked widened exact arithmetic;
- mean counts add independently;
- minima and maxima select by the frozen typed comparator;
- Boolean `any`/`all` combine by their respective identities; and
- exact distinct counts combine bounded canonical value sets or a provider-
  specific exact cardinality structure whose state format and maximum are
  declared.

Descriptor validation, comparator/profile lookup, arithmetic schema, and
partial-state compatibility are paid once per compiled plan/provider generation
under ADR-0129. They are never re-proved per row, group, measure, page item, or
partial merge. Runtime work still charges every actual row contribution and
state growth.

Persisted provider partial state is rebuildable but release-significant under
ADR-0124. A new state format needs an identity, fixtures, readable/writable
window, rebuild path, and retirement plan. WP-693 may keep a descriptor or
partial state transient only when no bytes persist and no executable artifact
needs to identify it.

### 6. Bound grouping and distinct cardinality independently

Row count, group count, distinct values per measure, aggregate state bytes,
arithmetic operations, scanned index/column rows, projected cells, and output
bytes are separate compiler and runtime budget dimensions. A page limit is not
a distinct-cardinality or group-state bound. Several distinct measures do not
silently share one allowance unless the compiler proves and charges a shared
canonical set.

Exact distinct aggregation compiles only when the ordinary source provides a
finite candidate maximum small enough for its set budget or the selected exact
provider advertises a maintained cardinality shape for the complete compiled
predicate. Budget exhaustion releases no partial aggregate or group. Excess is
a typed whole-query refusal with safe bounded diagnostics, never an approximate
answer.

Grouping remains over compiler-declared fields only. Multiple grouping sets,
rollup, cube, arbitrary expressions, and caller-selected dimensions remain
unavailable. Facets remain ADR-0130 provider measures and are not inferred from
group-by merely because their result shapes look similar.

### 7. Keep statistical, ordered-set, and collection folds out until real use

The registry design must be able to describe, but WP-693 through WP-696 do not
implement, variance, standard deviation, covariance, percentile/median,
histogram, approximate distinct, top-k, first/last, arg-min/arg-max, string/list/
JSON collection, or user-defined aggregates.

A non-normative descriptor sketch must sanity-check at least exact variance,
one approximate cardinality sketch, and one percentile/histogram provider. The
sketch records exact/approximate posture, numeric algorithm, merge state,
quality/error statement, empty/null behavior, determinism, bounds, provider
epoch, policy partition, and durable-state identity. Failure to express a shape
causes the registry to be revised on paper; it does not justify speculative
runtime, sketch, bridge, or persisted state.

Ordered selectors such as first, last, arg-min, and arg-max also require total
order and tie semantics. Where an existing bounded `order by ... take 1` query
returns the desired row, the compiler uses that clearer result-set operation
rather than adding a duplicate aggregate.

Unbounded collection-producing folds and application-supplied aggregate code
remain prohibited. No function may run target-language callbacks, floating-
point business arithmetic, regex, network, filesystem, clock, randomness, or
process-global mutation.

### 8. Sequence implementation behind current real blockers

WP-693 inventories every existing aggregate spelling, IR tag, descriptor,
evaluator, provider adapter, wire arm, generated type, and fixture; installs the
single semantic capability registry; and adds the three paper-only future
descriptor sketches. Existing bytes and behavior must remain unchanged.

WP-694 adds `count_present`, the two exact distinct counts, `mean`, `any`, and
`all` through grammar, formatter, resolved IR, reference evaluator, bounded
ordinary/grouped execution, result carriage, and generated clients. It may
split into interface-first commits but cannot ship a function on only one
public transport.

WP-695 exercises the registry through the existing columnar provider and the
exact-result provider at matched epochs, advertising only functions each can
serve exactly within its declared state and work bounds. It adds no cross-
provider bridge and no speculative persisted statistical state.

WP-696 supplies differential, policy/inference, recovery/compatibility,
external generic application, handbook, and final evidence. ADR-0150/WP-687's
binary membership and ADR-0151/WP-691's unary consume blocker remain earlier
alpha work and do not wait for this function expansion.

## Options Considered

1. **Keep only count/sum/min/max indefinitely:** rejected because non-null,
   distinct, average, and Boolean folds are routine application needs and would
   otherwise become client-side post-processing.
2. **Copy a SQL aggregate catalog and semantics:** rejected because SQL dialects
   differ on nulls, decimals, overflow, ordering, sketches, and user-defined
   code, and the resulting surface would not be compiler-bounded.
3. **Add functions independently to each executor:** rejected because result,
   precision, population, policy, epoch, and compatibility semantics would
   drift.
4. **Build statistical/sketch runtimes now:** rejected because no current real
   consumer selects their algorithm, accuracy, or durable state.
5. **Freeze a closed exact core plus provider capability tiers:** proposed
   because applications gain useful folds while future analytical functions
   have an explicit, evidence-driven extension path.

## Consequences

- RiffQL gains a practical exact aggregate core beyond its existing four
  functions without becoming SQL or an analytical callback runtime.
- Average remains exact and mergeable as total plus count; presentation
  rounding stays explicit rather than hidden in the database.
- Exact distinct work has honest cardinality/state bounds and cannot silently
  degrade to approximation.
- Ordinary, grouped columnar, and whole-result provider execution share
  semantics but keep their distinct population and scale contracts.
- Rich statistical and ordered-set functions remain deferred until real
  consumers can choose their durable and numerical semantics.

## Compatibility

This Proposed ADR itself changes no bytes or public behavior. Existing
`count`, `exact_count`, `sum`, `min`, `max`, group-by, aggregate response arms,
plan/module hashes, generated clients, columnar state, and provider descriptors
retain their exact meanings and encodings.

New source functions require additive grammar, formatter, resolved query IR,
canonical module/plan identity, result-schema, wire-carriage, and generated-
surface successors classified under ADR-0124. Old decoders remain for their
declared window. No old function tag or result arm may be reused for a new
meaning. `ExactMeanV1` and any persisted provider partial-state format require
registered identities and golden fixtures before activation.

WP-693 must first determine which capability-registry data is transient and
which enters a canonical executable or provider descriptor. Only the latter
creates a durable version-topology node. A transient registry refactor cannot
change existing plan hashes. A provider that lacks a newly required function
continues to advertise it as unsupported; runtime substitution is forbidden.

## Security

Every predicate field, group key, aggregate input, result field, and possible
plan family member is authorized before execution. Partition-scoped or policy-
aligned provider state is primary for whole-result measures. Row policy runs
before any contribution enters a group, distinct set, count, sum, extreme,
mean, or Boolean fold.

Compiler-owned minimum group size and inference classification may suppress or
refuse sensitive aggregates in addition to ordinary output limits. Neither
aggregate state nor partial values are released on refusal. Diagnostics expose
safe source symbols and bounded actual/maximum integers only; they do not expose
parameter values, group keys, distinct members, partial sums, extrema,
cardinality, hidden fields, policy exclusions, or provider statistics.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** callers choose only typed values
  for a named compiled query. Function, input field, grouping, population class,
  provider, exactness, precision, policy, epoch, and work bounds are compiler-
  sealed; unsupported combinations fail closed without client folds or scans.
- **Scale:** ordinary folds are bounded by their source collection; grouped and
  distinct state have independent caps; whole-result work is provider-declared,
  partition-scoped, and epoch-bound. No function requires database-wide memory,
  unbounded materialization, cross-partition work, or per-row descriptor proof.

## Testing

- Registry completeness tests enumerating every parser function, IR tag,
  result type, evaluator arm, provider capability, wire conversion, generator,
  and unsupported diagnostic.
- Differential reference tests for every function over empty, one-row,
  maximum-row, grouped, overflow, `NoValue`, duplicate, distinct-bound, and
  canonical-order corpora.
- Checked arithmetic and merge-law properties for count, sum, mean, extrema,
  Boolean identities, and exact distinct unions, including different merge
  partitions and compaction/rebuild schedules.
- Source-span snapshots for wrong input types, optional Boolean/numeric inputs,
  excessive groups/distinct state, unsupported population class, provider
  mismatch, and forbidden future functions.
- Columnar/exact-provider equivalence at the same epoch and policy partition;
  no page fold, cursor walk, post-policy contribution, or mixed-epoch result.
- Authorization, minimum-group, revocation, redaction, timing-class, and
  tenant-statistics isolation tests for aggregate inputs, groups, and results.
- Grammar/IR/descriptor/wire/generated-client golden fixtures plus old-reader,
  new-reader, rebuild, retirement, and plan/module identity rotation evidence.
- Architecture checks proving one semantic registry, pay-once descriptor
  validation, no per-row profile reconstruction, no target-language callback,
  and no external-framework branch.

## Requirements and Work Packages

- **Future requirements after exact acceptance:** `OQ-044` through `OQ-055`
- **Inventory, semantic registry, and future descriptor sketches:** WP-693
- **Exact core grammar/runtime/carriage/generated implementation:** WP-694
- **Columnar and exact-provider capability integration:** WP-695
- **Differential, security, compatibility, external, and documentation
  evidence:** WP-696

## Decision Deadline

Exact human acceptance is required before WP-693 changes aggregate capability
identity or WP-694 adds source syntax, IR tags, result values, or public
bindings. A new numerical algorithm, approximation, durable partial state,
provider bridge, policy posture, or population meaning requires separately
classified human review rather than a package-local extension.
