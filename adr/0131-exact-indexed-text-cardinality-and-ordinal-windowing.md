# ADR-0131: Exact Indexed Text Matching, Cardinality, and Ordinal Windowing

- **Status:** Accepted
- **Direction approved:** 2026-08-16 (maintainer, in session)
- **Exact text accepted:** 2026-08-16 (maintainer, as written)
- **Decision deadline:** Before WP-645 freezes an exact-text profile, provider
  state format, count plan, or ordinal public surface
- **Requires:** ADR-0051, ADR-0052, ADR-0053, ADR-0086, ADR-0108, ADR-0111,
  ADR-0117, ADR-0124, and ADR-0130
- **Defines or blocks:** WP-645 through WP-647 and Better Auth admin-profile
  acceptance

The maintainer accepted this exact text on 2026-08-16. This ADR is now
authoritative.

## Context

Better Auth's admin user-listing API combines four requirements in one request:
deterministic `contains`, `starts_with`, or `ends_with` text matching; ordinary
typed filters and sorting; numeric offset pagination; and an exact total over
the same filtered population. Current operational RiffQL provides indexed
binary prefix matching, cursor windows, and aggregates over an earlier bounded
collection. Adapter-side filtering, walking and discarding cursor pages, or
counting only the returned page would give incorrect results and dishonest
costs.

These requirements are not unique to authentication. Exact matching,
cardinality, facets, ranking, and ordinal windows are common result-set
operations for administrative lists, catalog search, columnar facets, and
future search projections. The durable format must therefore implement the
provider contract in ADR-0130 rather than encode Better Auth route details. The
application surface remains a finite compiler-generated named-query family.

## Proposed Decision

### 1. Add versioned exact text-match profiles, not regex or full-text search

An exact text-match profile freezes:

- input scalar type and maximum indexed value/needle bytes;
- canonical normalization (binary UTF-8 in the first profile);
- `equals`, `starts_with`, `ends_with`, and `contains` truth semantics;
- empty-needle behavior, missing/null behavior, and comparison domain;
- provider-state identity and rebuild/migration rules; and
- maximum write amplification, retained bytes, candidate work, and diagnostic
  size admitted by the declaration.

For `binary_utf8_v1`, values and needles are valid canonical UTF-8 and matching
is exact over their UTF-8 byte sequences. `starts_with`, `ends_with`, and
`contains` have their ordinary non-regex byte-sequence meanings. An empty
needle is rejected at bind time with a typed error rather than matching every
row. Missing and null never match. There is no locale, collation, wildcard,
escape syntax, regex, tokenization, stemming, fuzzy matching, scoring, or
corpus statistic.

The semantic profile is independent of the provider's physical exact-index
algorithm. A provider may change physical implementation only when canonical
results, declared work bounds, and provider-state compatibility remain proven;
otherwise it needs a new provider-state identity and the ADR-0124 transition.
ADR-0092 full-text search remains a distinct ranked provider even when a future
named query uses the same ADR-0130 pipeline shapes.

### 2. Require a declared exact provider and reject scan substitutes

Each searchable field and finite predicate/order family must name a compiler-
validated exact-text provider profile. Compilation fails with a source-spanned
diagnostic when the field bound, partition route, policy mode, requested
operator, ordering, exact measure, or ordinal capability is not supported by
that profile.

Execution cannot substitute an entity scan, client filtering, regex engine,
prefix approximation, page fold, cursor walk, per-row remote fetch, or
materialize-all sort/count. Static plan charge and runtime fuel account for the
provider's declared worst-case index work and output independently. Excessive
value bounds, index amplification, predicate combinations, or result work are
compiler errors, not operator-tunable unsafe modes.

### 3. Define exact whole-population cardinality as a provider measure

`exact_count` measures the complete population admitted by the selected named
predicate and policy shape at the result set's exact snapshot epoch. It is not
`count()` over a prior `take`, page, candidate cap, or approximate ranking
window. It returns a checked unsigned cardinality or a closed typed overflow/
unavailable outcome and materializes neither rows nor a row-sized intermediate
collection.

The provider must maintain or derive index cardinality sufficient for every
compiler-enumerated predicate combination it advertises. Partition scope or a
policy-aligned subpartition must exclude unauthorized rows before the measure;
bounded row admission may be used only when the provider can prove exact
cardinality within its declared candidate ceiling. A provider unable to prove
this advertises no exact-count capability and the query does not compile.

A query may request a page and `exact_count` together. Both are evaluated from
one result-set plan and one ADR-0130 epoch proof so their filters, policy shape,
text profile, and snapshot cannot drift. Returning them in separate wire fields
does not authorize separate snapshots.

### 4. Support numeric offset through indexed ordinal selection

Numeric offset is an ordinal within the named query's fully filtered, totally
ordered result set at its selected snapshot. Every order appends a declared
unique deterministic tie-breaker. Offset zero selects the first row; an offset
at or beyond exact cardinality returns an empty page; arithmetic overflow or a
value outside the compiled parameter bound is a typed bind error.

The provider must translate the ordinal to an index position under its declared
work bound. It cannot walk and discard earlier pages or rows. Plan cost states
the provider's worst-case seek/intersection work, and runtime fuel verifies it.
An advertised ordinal capability therefore implies the maintained cardinality
or order-statistic information necessary for that predicate/order family; it
does not imply constant time for every physical engine.

Offset stability is snapshot stability, not a promise across unrelated current-
snapshot requests. A combined page/count request is internally snapshot exact.
An opaque continuation may bind that snapshot under ADR-0130 retention rules;
a later numeric-offset request that opens a new result set may observe newer
writes. Generated documentation must say this explicitly.

### 5. Keep the application surface finite and generated

The compiler enumerates every allowed combination of optional text search,
typed filter, order, page limit, and offset for a named query. Requests carry
only typed field values, closed generated operator choices when the source
declares more than one, bounded limit, and bounded offset. They never carry a
field name, index name, provider name, arbitrary operator, sort expression,
predicate AST, facet expression, or consistency downgrade.

Generated Rust, Go, TypeScript, and Python SDKs expose the same finite family
and typed result containing the page, exact total when declared, and snapshot/
continuation metadata. CLI and MCP use that same application service and plan;
they do not gain an ad hoc search endpoint. Secret-output authority remains
owned by ADR-0128 and is unaffected by whether a row was found through this
provider.

### 6. Use Better Auth as acceptance evidence, not the design center

WP-647 must prove the real Better Auth admin route for `contains`,
`starts_with`, and `ends_with`, its declared typed filters and total orders,
numeric offsets including zero/end/out-of-range cases, and exact totals at the
same snapshot. It must include adversarial pagination, concurrent-write,
authorization, tenant-isolation, null/missing, Unicode-byte, and maximum-bound
cases. No adapter-side filtering, count, sort, page walk, or query AST is
permitted.

The exact provider remains described in generic predicate, measure, order, and
window terms. Organization/two-factor or later profiles may reuse it only by
declaring their own finite plan families and policy partitions; Better Auth
cannot add route-specific runtime branches to the provider.

### 7. Reserve facets and ranked-search composition without implementing them

The provider reports exact cardinality through ADR-0130's whole-result measure
shape, which can later carry compiler-declared facets. WP-645 through WP-647 do
not add a public facet operator unless a real accepted profile requires one.
They add no BM25 runtime, full-text token index, vector/text bridge, columnar
bitmap transfer, or cross-provider executor. A future composite must satisfy
ADR-0130's real-consumer, bridge, epoch, policy, and cost requirements.

## Options Considered

1. **Filter and count in the Better Auth adapter:** rejected because pages and
   totals become incorrect and policy/cost enforcement moves outside RiffDB.
2. **Walk cursor pages to emulate offset:** rejected because work grows with the
   offset, artificial page ceilings leak into semantics, and the public safety
   boundary becomes dishonest.
3. **Add route-specific substring and count calls:** rejected because it freezes
   the use case into durable state and generated APIs.
4. **Add a generic exact provider for finite compiled result sets:** proposed
   because the provider remains bounded and safe while serving a real adapter.

## Consequences

- Better Auth admin listing can have exact server-side semantics and honest
  pagination/count behavior.
- Exact substring support has bounded index/storage cost paid at declaration
  and write/rebuild time rather than hidden unbounded read scans.
- Numeric offset becomes available only on plans with indexed ordinal proof;
  cursor pagination remains the default for ordinary operational queries.
- The exact provider may require more derived storage and write amplification;
  both are declared, bounded, observable, and rebuildable.
- Facets, BM25, and cross-provider composition remain deferred.

## Compatibility

This Proposed ADR changes no current bytes or public behavior. If accepted,
WP-645 introduces new exact-text semantic and provider-state identities,
compiler IR/module/plan identities as required, generated surface revisions,
and compatibility fixtures. All are registered under ADR-0124 before merge.
Existing `text_key` prefix indexes and query modules remain readable and byte-
identical; they do not silently acquire substring, exact-count, or ordinal
capability. Migration/rebuild is explicit, receipted, and fail-closed, with old
decoders retained under the topology window.

## Security

Exact indexes and cardinality/order statistics are partition scoped by default.
Unauthorized rows cannot affect result presence, exact total, ordinal, rank,
cursor, work-class choice, diagnostics, or timing outside the declared bounded
policy class. Public errors carry closed reason, bound, and safe remedy fields,
not hidden values, tenant sizes, index layouts, or policy facts. Secret fields
remain redacted unless a named query has ADR-0128 authority.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** callers can express only a
  compiler-generated finite query with typed bounded inputs. They cannot request
  scan fallback, client filtering, arbitrary offset work, regex, hidden fields,
  a weaker snapshot, or an unauthorized count. Unsupported combinations fail
  compilation or binding with typed outcomes.
- **Scale:** index state and write amplification are bounded by declared field,
  partition, and profile limits. Counts and ordinal seeks do not materialize or
  scan the matching population, and execution requires neither database-wide
  memory nor co-located authoritative storage. Rebuild and retention remain
  bounded derived-consumer operations.

## Testing

- Truth-table and property fixtures for binary UTF-8 equals/prefix/suffix/
  contains, including empty, null/missing, multibyte, boundary, and maximum-
  length cases.
- Exact-provider/reference-evaluator equivalence across randomized writes,
  deletes, rebuilds, compaction, crash boundaries, and matched epochs.
- Full-population count and ordinal-selection properties against an independent
  materialized test oracle, while architecture tests forbid production
  materialization/page walking.
- Policy-partition adversarial cases proving denied rows affect no count,
  ordinal, cursor, work class, or timing class.
- Compiler span snapshots for every unsupported profile/operator/order/policy/
  bound combination and compatibility fixtures for every new identity.
- Cross-language SDK corpus and real Better Auth admin-route conformance.

## Requirements and Work Packages

- **Requirements:** `OQ-025` through `OQ-030`
- **Provider and compiler:** `WP-645`
- **Execution and generated surfaces:** `WP-646`
- **Final Better Auth evidence:** `WP-647`

## Decision Deadline

Exact human acceptance is required before WP-645 freezes the semantic profile,
provider-state format, query IR, module/plan hash carriage, topology entries, or
generated application surface. A physical design that cannot meet exact count
or ordinal bounds must return for review rather than weaken those semantics.
