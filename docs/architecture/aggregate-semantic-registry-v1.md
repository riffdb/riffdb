# Aggregate semantic registry v1

Status: WP-693 checked architecture evidence; existing behavior only.

ADR-0152 assigns aggregate meaning to one closed compiler-owned registry. The
registry is implemented in `riffdb-types::aggregate_semantic_registry_v1` so
the source parser, typed IR, compiler, authoritative evaluator, and existing
columnar provider can share the same vocabulary without a dependency cycle.
It is not request data: an application can select only a function and field
written in a compiled named RiffQL document.

## Existing semantic inventory

| Semantic | Source and parser | Resolved and durable IR | Exact evaluator/result | Existing provider/result | Module, wire, generated, and fixtures |
|---|---|---|---|---|---|
| `count` | `AggregateFunction::Count`; registry spelling `count`; zero fields | `OperationalAggregateFunctionV1::Count`; frozen tag `1`; `u64` schema | authoritative bounded/grouped fold; `QueryAggregateCell::Canonical(U64)`; empty `0` | `AggregateOp::Count`; `AggregateValue::Count`; columnar whole/group fold | version-3 query module retains tag/schema; named result field is ordinary canonical `u64`; every SDK and MCP schema derives that named schema; operational syntax/compiler/executor/module and columnar acceptance fixtures |
| `exact_count` | `AggregateFunction::ExactCount`; registry spelling `exact_count`; zero fields | retained as whole-result intent and normalized to the existing count metadata only after exact-family validation; no operational tag of its own | exact-result provider returns the complete admitted `u64` total at the page epoch; never a page fold | exact text/predicate/result-set plan families; provider cardinality precedes ordinal windowing | exact-result query module plan and named result `u64`; ordinary generated field decoding; exact text/predicate module and provider fixtures |
| `sum` | `AggregateFunction::Sum`; registry spelling `sum`; one exact numeric field | `OperationalAggregateFunctionV1::Sum`; frozen tag `2`; widened decimal schema at source scale | checked `i128` coefficient plus compiler scale; `QueryAggregateCell::ExactDecimal`; empty exact zero | `AggregateOp::Sum`; `AggregateValue::Sum(i128)`; columnar whole/group fold | version-3 module schema emits the existing decimal shape; Rust/Go/TypeScript/Python use their existing exact-decimal value; operational and columnar aggregate fixtures |
| `min` | `AggregateFunction::Min`; registry spelling `min`; one ordered scalar | `OperationalAggregateFunctionV1::Min`; frozen tag `3`; optional input scalar schema | frozen typed comparison; canonical scalar cell; outer absence only for an empty population | `AggregateOp::Min`; `AggregateValue::Scalar`; columnar whole/group fold | version-3 module preserves optional scalar schema; generated optional canonical value; empty/`NoValue` and columnar fixtures |
| `max` | `AggregateFunction::Max`; registry spelling `max`; one ordered scalar | `OperationalAggregateFunctionV1::Max`; frozen tag `4`; optional input scalar schema | frozen typed comparison; canonical scalar cell; outer absence only for an empty population | `AggregateOp::Max`; `AggregateValue::Scalar`; columnar whole/group fold | version-3 module preserves optional scalar schema; generated optional canonical value; empty/`NoValue` and columnar fixtures |

`group by` is a population/result-shaping construct, not a sixth function. Its
resolved keys remain `OperationalAggregateGroupKeyV1`, groups retain canonical
encoded-key order, and every measure inside a group resolves through the same
registry. Whole-set folds emit one aggregate record even for empty input;
grouped folds emit no record for empty input.

The event-derived materialized-projection DSL also has durable
`ProjectionAggregation::{Count, Sum}` tags. That is a separate write-time
delta/state format, not a RiffQL query aggregate or a whole-result provider
capability: its `sum` retains the declared measure type and already has its own
release-significant topology. The overlapping spellings are inventoried here
to prevent accidental substitution. They are not silently relabeled as the
RiffQL `sum` descriptor; a future adapter may advertise a registry semantic
only after it proves exact result conversion, bounds, epoch, and policy.

The generated clients do not contain target-language aggregate algorithms.
They receive the compiler-resolved named result schema and assemble the same
ordinary `u64`, exact-decimal, optional scalar, record, and list facades used
elsewhere. The service/wire boundary likewise carries typed result fields, not
a caller-selected aggregate function or provider-specific partial state.

## Descriptor and validation ownership

Every descriptor freezes stable identity, source spelling and arity, accepted
input class, `NoValue`, empty result, result schema, partial state, arithmetic,
bounded/grouped/whole-result eligibility, independent budget sources, and
policy posture. Numeric maxima remain sealed in the complete compiled plan:
the registry says which dimensions require a maximum, while the source bound,
group clamp, provider declaration, and output envelope supply the exact finite
values.

Descriptor validation and mapping to a provider subset are pay-once work for a
compiled plan and provider generation. They are never repeated per row, group,
measure, page item, or partial merge. Policy-aligned partitioning is the
primary provider enforcement mode; bounded row admission is secondary and
still occurs before contribution, count, grouping, ordering, or limiting.
Refusal releases no partial aggregate.

## Durable-state classification

The v1 registry and its descriptors are transient compiler vocabulary. They
are neither serialized nor hashed and therefore add no version-topology node.
The existing operational tags `1` through `4`, query-IR/module versions, plan
hashes, result/wire values, generated artifacts, and provider-state identities
remain byte-exact.

An in-memory partial accumulator used only during one bounded evaluation is
also transient. A future executable artifact that embeds a registry identity,
or rebuildable provider state that persists a partial-state representation, is
release-significant under ADR-0124. It must receive a topology identity,
golden fixtures, reader/writer windows, rebuild/catch-up behavior, and a
retirement plan before it is implemented.

## Non-normative descriptor sketches

These sketches test descriptor expressiveness only. None is a RiffQL spelling,
runtime arm, dependency, provider bridge, executable artifact, or persisted
state.

### Exact variance sketch

- posture: exact
- numerical algorithm: compiler-fixed mergeable integer/fixed-decimal moments; no floating point and no implicit division or rounding
- merge state: checked `{count: u64, sum: widened exact, sum_squares: widened exact}` with a separately reviewed overflow proof
- quality/error statement: exact or typed whole-result refusal; never an approximate answer
- empty/null behavior: explicit exact empty record; optional inputs require a separately accepted `NoValue` rule
- determinism: canonical typed contributions and merge order-independent checked state
- bounds: input rows, arithmetic operations, state bytes, groups, output bytes, and diagnostics independently compiler-sealed
- provider epoch: every partial and returned measure belongs to one negotiated provider epoch
- policy partition: policy-aligned partition primary; admission before contribution
- durable-state identity: required before any persisted moments state; absent from WP-693

### Approximate cardinality sketch

- posture: approximate
- numerical algorithm: illustrative fixed-precision HyperLogLog-family descriptor only; algorithm and hash domain would require later acceptance
- merge state: fixed register count and width with identical algorithm/version/precision/hash identity
- quality/error statement: compiler-owned confidence/error declaration carried distinctly from exact cardinality; never returned under `count_distinct`
- empty/null behavior: estimate zero for empty input; `NoValue` inclusion is an explicit semantic identity choice
- determinism: canonical typed value bytes and a versioned hash domain; deterministic register merge
- bounds: precision, register bytes, input work, groups, output bytes, and diagnostics compiler-sealed
- provider epoch: sketch, page, facets, and related measures must use one negotiated provider epoch
- policy partition: statistics isolated by policy-aligned partition; no cross-partition merge visible to a narrower principal
- durable-state identity: mandatory algorithm/precision/hash/state identity plus rebuild and retirement before persistence; absent from WP-693

### Percentile and histogram sketch

- posture: approximate ordered-distribution summary (an exact bounded variant would need a distinct identity)
- numerical algorithm: illustrative deterministic fixed-compression quantile sketch plus compiler-fixed histogram boundary profile
- merge state: bounded ordered centroids or fixed buckets with versioned comparator, compression, boundary, and tie rules
- quality/error statement: explicit rank-error or bucket-bound statement; no exact-percentile claim and no silent interpolation
- empty/null behavior: explicit absence/empty buckets; optional values require a separately accepted participation rule
- determinism: frozen typed ordering, canonical merge normalization, and compiler-fixed requested quantiles/boundaries
- bounds: centroids or buckets, state bytes, merge work, groups, output cells/bytes, and diagnostics compiler-sealed
- provider epoch: distribution state and all composed query stages share one negotiated provider epoch
- policy partition: policy-aligned partition primary; no post-aggregation filtering or cross-policy statistics
- durable-state identity: mandatory algorithm/compression/boundary/state identity plus compatibility and rebuild evidence before persistence; absent from WP-693

The sketches deliberately specify no bitmap transfer or cross-provider bridge.
A bridge remains out of scope until a real consumer requires it.
