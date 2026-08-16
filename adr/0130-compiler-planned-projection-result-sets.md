# ADR-0130: Compiler-Planned Projection Result Sets and Snapshot-Aligned Composition

- **Status:** Proposed
- **Direction approved:** 2026-08-16 (maintainer, in session)
- **Exact text accepted:** No
- **Decision deadline:** Before WP-644 freezes a provider descriptor or any
  durable projection-result identity
- **Requires:** ADR-0002, ADR-0051, ADR-0053, ADR-0086, ADR-0087, ADR-0091,
  ADR-0092, ADR-0111, ADR-0124, and ADR-0129
- **Defines or blocks:** WP-644 and the durable foundation used by WP-645

Direction approval authorizes this draft and the associated planning records.
This ADR is not authoritative until a human accepts its exact text and changes
the status to Accepted.

## Context

RiffDB now has three concrete result-shaping needs with different physical
engines:

- the existing columnar worker filters, orders, groups, and aggregates derived
  rows;
- the per-organization vector tier performs exact or approximate nearest-
  neighbor ranking; and
- Better Auth needs a new exact index that combines deterministic substring
  predicates, exact full-population cardinality, total ordering, and ordinal
  windowing.

Treating each as an unrelated query exception would freeze overlapping durable
descriptors, snapshot rules, policy contracts, and generated surfaces. Under
ADR-0124, later replacing a narrow durable text-index format would require the
full reader, migration, fixture, and retirement ceremony. Conversely, building
an imagined universal search runtime before a second real consumer exercises it
would create abstraction without evidence.

The shared foundation therefore has to be provider-shaped where identities are
durable, but deliberately small in runtime scope. It also has to solve an
availability issue: a query involving more than one rebuildable provider can
serve only an epoch retained by every participant. Typed refusal is safe, but
routine refusal while one consumer catches up is an operational defect rather
than an acceptable steady state.

## Proposed Decision

### 1. Compile one closed result-set pipeline

A named RiffQL query may compile a result set through these ordered stages:

1. candidate generation;
2. policy admission and declared filtering;
3. optional ranking or total ordering;
4. whole-result measures such as exact count or facets;
5. cursor or ordinal windowing; and
6. typed projection of the declared output.

Every stage is optional only when the compiler-generated plan says so. The
request carries typed values and presence choices, never a provider, index,
field, operator, stage, score function, cost hint, fallback, or arbitrary query
tree. The compiler selects and pins the provider and its exact capability
descriptor in the module and plan identities. Runtime provider choice, silent
fallback, client filtering, N+1 fetch, and materialize-all substitution are
forbidden.

A single provider may implement several stages. This ADR does not require a
separate engine call per stage and does not make stage boundaries public RPCs.
The pipeline is semantic compiler IR, not an application-programmable dataflow
language.

### 2. Freeze a sealed, versioned provider descriptor before provider state

The compiler consumes a canonical `ProjectionProviderDescriptorV1` with:

- stable provider kind and descriptor-version identities;
- exact versus approximate result semantics and any declared quality target;
- closed predicate, candidate, ranking/order, whole-result measure, facet,
  window, and output capabilities;
- required partition and policy-enforcement mode;
- freshness class, servable-snapshot retention obligation, and rebuild/catch-up
  behavior;
- static work, input, output, cardinality, state-amplification, and diagnostic
  ceilings; and
- the provider-owned rebuildable-state format identities and compatibility
  evidence required by ADR-0124.

Unsupported capability is explicit and fails compilation. Approximate
candidate or rank semantics cannot satisfy an exact-count requirement merely
because the provider returns a numeric value. A descriptor is sealed first-
party compiler input; applications and deployment requests cannot supply or
alter one.

The canonical descriptor and every compiler artifact that embeds its digest
are release-significant formats. WP-644 must register them in the ADR-0124
version topology before they merge. A provider-owned persisted projection
format is rebuildable rather than authoritative, but it still requires a
versioned identity, fixtures, readable/writable windows, and typed rebuild or
refusal. Internal runtime witnesses and call plumbing are not serialized and
must remain minimal until another real engine requires more.

### 3. Validate the descriptor against real engines, not an imagined union

WP-644 must adapt and test both existing provider families:

- the columnar provider exercises partitioned filtering, total order,
  aggregation/facet-shaped measures, exact reference evaluation, and frontier
  publication; and
- the exact/ANN vector provider exercises ranked candidates, declared
  approximation, recall evidence, per-organization statistics isolation,
  filtering before ranking, and matched-frontier reference evaluation.

Each provider advertises only behavior it actually implements. Their union is
not a requirement that either implement the other's features.

ADR-0092 BM25 full-text search is represented only by a non-normative
descriptor sketch proving that the closed shapes can name lexical candidates,
fixed-point ranking, filters, whole-result measures, partition-local
statistics, and exact/approximate posture. WP-644 must add no BM25 runtime,
durable BM25 artifact, adapter, fixture, or bridge. If the sketch cannot be
expressed, the descriptor is revised before acceptance rather than worked
around in speculative code.

### 4. Make partition-scoped indexes the primary policy mode

Provider policy modes are ranked and compiler-visible:

1. **Partition-scoped:** the index and every result-shaping statistic are
   physically scoped by the complete capability/organization partition. This
   is the primary mode for ranking, exact counts, facets, ordinals, cursors,
   work-class behavior, and timing isolation.
2. **Policy-aligned subpartition:** a compiler-proved finite policy class selects
   a separately maintained subpartition or index whose statistics contain only
   rows admissible to that class.
3. **Bounded row admission:** a row predicate may refine a statically bounded
   candidate set before ranking or measures. It is secondary only and cannot
   justify a full-partition scan or let rejected rows affect counts, facets,
   scores, ranks, offsets, cursors, work-class selection, or public timing
   beyond an accepted bounded policy class.

Compilation rejects an exact whole-result measure or ranking plan when its
available policy mode cannot uphold those invariants. Post-ranking or post-
count policy filtering is forbidden. This ordering strengthens, and does not
replace, ADR-0111 request-time authorization and field-output checks.

### 5. Negotiate the newest common servable epoch once per result set

Every participating provider publishes, for one database history incarnation
and projection generation, a contiguous inclusive interval of exact servable
commit epochs `[floor, ceiling]`. It also declares a minimum retained interval,
maximum admitted catch-up divergence, and the bounded cursor lifetime or epoch-
lease rule its deployment profile can sustain. A provider that retains only
one snapshot publishes a one-epoch interval and cannot satisfy a plan whose
declared window requires more.

For a plan with participants `P`, the service computes:

```text
common_floor   = max(P.floor)
common_ceiling = min(P.ceiling)
```

It selects `common_ceiling`, the newest common epoch, only when
`common_floor <= common_ceiling`, every participant proves that exact epoch,
and the selected epoch satisfies the named query's causal or staleness policy.
The resulting epoch proof binds history incarnation, projection generations,
provider descriptor digests, policy shape, plan identity, and the selected
epoch. No provider may combine rows, statistics, scores, or measures from
different epochs and label the result consistent.

If no acceptable intersection exists, execution returns a closed typed
`projection_diverged`, `snapshot_retired`, or `freshness_unsatisfied` outcome
with bounded safe retry/reset guidance. It does not serve a mixed epoch, walk
authoritative history, silently weaken freshness, or fall back to another
provider. Cursor continuation whose bound epoch has retired returns the
declared typed reset/refusal rather than shifting the result set.

Retention and catch-up are availability contracts, not documentation. A
provider that repeatedly breaches its declared intersection or divergence SLO
becomes degraded or unavailable in projection health, catch-up is prioritized
under fixed resources, and new work may receive bounded typed backpressure.
Routine typed refusal under an admitted workload fails acceptance; refusal is
reserved for declared overload, rebuild, retention expiry, or fault behavior.

### 6. Pay validation and epoch proof costs once

Canonical descriptor decoding and compatibility validation occur once per
deployment/catalog generation. The resulting provider-plan witness is keyed by
the exact catalog generation, module/plan identity, descriptor digest,
provider-generation identity, and history incarnation. It is never rebuilt per
row, candidate, measure, page item, or provider call.

Epoch negotiation occurs once when a bounded result set is opened. An opaque
cursor or continuation binds the epoch-proof identity and performs only bounded
identity, lifecycle, and freshness validation; it does not re-prove descriptor
compatibility or re-negotiate a nearby epoch on each page. This optimization
never caches authority: every request and every existing safe point still
performs fresh authentication, capability-revision, row/field-policy, and
revocation checks. Changed authority denies or selects only a separately
compiled policy shape; it cannot reuse a wider proof.

Architecture tests must enumerate production descriptor and epoch-proof
construction sites and reject any per-row or repeated page-operation
construction, applying ADR-0129's pay-once rule to this plane.

### 7. Defer cross-provider bridges until a real plan needs one

The descriptor can state whether a provider consumes a compiler-owned bounded
candidate representation, but WP-644 builds no bitmap transfer, ID bridge,
cross-provider materialization, or composite executor. The columnar and vector
engines are validated independently against the same descriptor and epoch
rules.

Actual composition requires a later accepted amendment naming a real compiled
query, an explicit bounded bridge representation, cardinality and byte limits,
policy ownership, epoch transfer proof, cost accounting, compatibility
identity, and independent reference evaluator. A bridge cannot be inferred
from two descriptors advertising superficially compatible types.

### 8. Preserve standing delivery priority

Drafting and accepting this paper decision may proceed in parallel. WP-644
implementation must follow WP-641 through WP-643 and the standing c=32, unary,
and ADR-0127 priority gates unless the maintainer explicitly reprioritizes them.
WP-644 is limited to the descriptor/format slice, the two real provider
adapters, and reference conformance; it may not grow a universal runtime or
bridge.

If ADR-0131 becomes alpha critical, the foundation must be reduced to that
least sufficient durable slice rather than delay alpha with optional runtime
plumbing. Omitting a durable identity or compatibility obligation to save time
is not permitted; any conflict between these constraints returns for human
review.

## Options Considered

1. **Build a Better-Auth-specific substring/count/offset index:** rejected
   because it would freeze overlapping durable semantics and force later
   retirement when columnar, vector, or full-text composition needs the same
   concepts.
2. **Build a universal federated search runtime now:** rejected because no real
   plan needs a cross-provider bridge and imagined consumers do not justify the
   runtime or durable surface.
3. **Keep independent engines behind a small compiler-owned descriptor and
   shared epoch/policy rules:** proposed because the durable shape is coherent
   while runtime work remains evidence-driven.
4. **Allow runtime provider selection or fallback:** rejected because cost,
   precision, policy, freshness, and result semantics would vary outside the
   reviewed plan identity.

## Consequences

- New providers pay one explicit compiler, policy, freshness, bounds, and
  compatibility integration cost instead of inventing new query semantics.
- Existing columnar and vector providers gain conformance adapters but need not
  implement unsupported capabilities or a shared execution runtime.
- Exact count, facets, ranking, and ordinal positioning become first-class
  result-set capabilities rather than ad hoc folds over returned pages.
- Snapshot retention and divergence become measurable provider availability
  obligations.
- Cross-provider execution and BM25 implementation remain explicitly deferred.

## Compatibility

This Proposed ADR changes no current bytes or public behavior. If accepted,
WP-644 introduces a new canonical descriptor identity and provider-generation
fixtures under ADR-0124. It must use a least-sufficient writer policy and
register every embedded executable/application artifact and persisted
rebuildable provider-state identity before merge. Existing query modules,
projection state, cursors, and provider formats remain byte-identical until a
receipted compiler campaign explicitly upgrades them; there is no implicit
decoder fallback.

## Security

Providers are unprivileged derived consumers. They receive only the compiler-
proved partition, policy mode, fields, bounds, and epoch needed by the plan and
cannot widen capability or access authoritative storage directly. Partition-
scoped statistics are the primary inference boundary. Closed public lifecycle
outcomes reveal no row, hidden provider, capability, index-layout, or tenant
cardinality. Fresh request authorization remains outside reusable descriptor
and epoch proofs.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** applications select only a named
  generated query and typed parameters. They cannot choose providers, bridges,
  precision, policy mode, epoch, work budget, scan, fallback, or unsafe result
  shaping. Compilation and execution fail closed when the declared guarantees
  cannot be met.
- **Scale:** descriptors and proofs have fixed size; provider work is partition-
  scoped and statically charged; epoch negotiation is proportional only to the
  compiler-fixed provider count. The design requires no co-located
  authoritative storage, database-wide memory, full-state rewrite, or
  cross-provider materialization.

## Testing

- Canonical descriptor encode/decode/hash fixtures and topology drift checks.
- Independent columnar and vector descriptor conformance suites, including
  explicit unsupported-capability cases.
- Reference-evaluator equivalence at matched frontiers and adversarial
  partition/policy isolation cases.
- Epoch-interval intersection properties, stale-incarnation and retired-epoch
  refusal, lag/rebuild schedules, and sustained-divergence health acceptance.
- Architecture checks proving descriptor validation is catalog-generation
  scoped, epoch proof is result-set scoped, and neither occurs per row/page.
- A compile-only non-normative BM25 descriptor sketch and a negative test that
  no BM25 runtime or bridge artifact is linked into WP-644.

## Requirements and Work Packages

- **Requirements:** `OQ-017` through `OQ-024`
- **Defines or blocks:** `WP-644`
- **First consuming provider:** `WP-645`
- **Final evidence:** `WP-647`

## Decision Deadline

Exact human acceptance is required before WP-644 freezes the descriptor,
provider-generation identity, topology entry, or compiler-plan carriage. The
maintainer must separately approve any implementation scope that adds a cross-
provider bridge or changes transaction, policy, freshness, or durable format
semantics.
