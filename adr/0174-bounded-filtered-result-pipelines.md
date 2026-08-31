# ADR-0174: Bounded Filtered Result Pipelines

- **Status:** Proposed
- **Direction approved:** 2026-08-31
- **Exact text accepted:** No
- **Acceptance reference:** The maintainer approved the bounded candidate-set,
  long-value pattern-provider, and higher page-ceiling direction in the current
  Codex session and requested the correct holistic capability rather than an
  adapter workaround. This exact record still requires review.
- **Decision deadline:** Before RiffQL admits a candidate binding, the global
  query row ceiling changes, or a long-value pattern-provider identity is
  frozen
- **Requires:** ADR-0051, ADR-0052, ADR-0053, ADR-0055, ADR-0086, ADR-0108,
  ADR-0111, ADR-0124, ADR-0130, ADR-0131, ADR-0134, ADR-0150, ADR-0158,
  ADR-0159, ADR-0164, ADR-0167, ADR-0172, and ADR-0173
- **Amends if accepted:** ADR-0131's exact-text implementation ceiling,
  ADR-0150's relationship-composition extension boundary, ADR-0158 and
  ADR-0167's 499-row global page ceiling, and ADR-0159's larger-page adapter
  exception
- **Defines or blocks:** Proposed WP-733 through WP-738

Direction approval is recorded, but this record is not authoritative until the
maintainer accepts its exact text. The implementation packages may add failing
fixtures and inventory evidence but may not freeze the public or durable
identities below before that acceptance.

## Context

An MLflow tracking-store implementation supplies the real consumer anticipated
by `OQ-042`. Its experiment search applies an AND-conjunction of tag predicates
before a caller-selected order over one of four root attributes and before
limit/cursor selection. RiffDB can now intersect tag digests through dependent
key batches, but that mechanism requires every dependent batch to order
ascending by exactly its joining field. It therefore cannot finish with the
root order required by the application.

Removing that compiler diagnostic would not make the query correct. Each
intermediate `many` binding has an ordinary page `take`; truncating either tag
population before intersection can omit a root that belongs in the final page.
Filtering or sorting an already paged root result, walking pages without a
common snapshot, or copying every possible root-order component into every tag
row is observably wrong. It can omit results, return the wrong cursor, or turn
an unbounded application vocabulary into write amplification.

The same consumer exposes a distinct physical limitation. MLflow permits
experiment names through 500 bytes, experiment tag values through 5,000 bytes,
and run tag values through 8,000 bytes. `unicode_fold_v1` correctly charges its
maximum eighteen-fold expansion, while an operational key remains capped at
4,096 bytes. The exact-text V1 provider is capped at 256 matched bytes and
materializes every substring, so increasing that constant to 8,000 would
permit more than thirty-two million substring terms for one row. Tokenized
search cannot substitute: analyzed term membership is not exact SQL
`LIKE`/`ILIKE` prefix, suffix, substring, `%`, or `_` behavior.

Finally, `Limit<MAX>` admits at most 499 result rows because it shares a
500-row physical scan ceiling with one continuation probe. The ceiling is much
lower than legitimate application and compatibility pages; MLflow permits
50,000 experiments in one requested page. RiffDB already has independent
encoded-result, request, policy, hydration, probe, and storage-byte budgets.
A universal 499-row ceiling rejects narrow results that remain inside every
more meaningful budget.

These are one search-pipeline problem: produce one complete bounded and
authorized candidate population, impose the compiled root total order, and
only then select and transport a result page. Solving only the reproduced tag
shape would leave text predicates, larger pages, null/existence, and combined
provider searches to encounter the same boundary separately.

## Proposed Decision

### 1. Add one non-output `candidates` binding

RiffQL adds a query-only binding whose conceptual source form is:

```riffql
candidates matching_experiments: Experiment.experiment_id
    from intersect {
        ExperimentTag.experiment_id using by_tag_digest
            where scope == $scope
                && tag_key == $first_key
                && value_digest == $first_digest,
        ExperimentTag.experiment_id using by_tag_digest
            where scope == $scope
                && tag_key == $second_key
                && value_digest == $second_digest,
    }
    within 65535
    else IntegrityFailure

many experiments from Experiment
    where scope == $scope
        && experiment_id in matching_experiments
    order by last_update_time desc, experiment_id asc
    take $limit after $cursor
    else IntegrityFailure
```

The implementation may refine punctuation during the syntax-fixture stage,
but it may not change the semantics below without returning this record for
review.

A candidate binding is not a returned collection, page, join row, temporary
entity, application value, or general set expression. It contains only the
canonical complete root keys produced by compiler-selected access sources. It
cannot appear in an outcome, nested result, aggregate input, command,
projection declaration, target-language value, or caller-supplied parameter.

V1 admits exactly:

- a single candidate source;
- finite intersection of two through eight sources;
- finite union of two through eight sources when a named predicate family
  requires OR; and
- finite difference of one positive source and one through eight negative
  sources for declared inequality, missing, or `NOT LIKE` semantics.

Each source is one declared ordinary exact index, exact-text provider,
long-pattern provider, or tokenized provider that emits the same type-exact
root key. A source may select a relationship row and project its declared
same-partition foreign key only when the contract proves that mapping. Runtime
field names, entity names, indexes, providers, operators, set shape, source
count, order, cost, and fallback remain impossible.

Every source and the combined binding carry independent positive maxima for:

- source matches and source-key bytes;
- distinct candidate keys and candidate-key bytes;
- provider postings or ordinary index entries inspected;
- relationship rows, target fan-out, probes, and point hydrations;
- policy decisions and policy-visible intermediate keys;
- normalized text, verification bytes, and pattern-machine work where used;
- sort rows, sort-key bytes, result rows, and encoded result bytes; and
- provider epochs, retained snapshot state, cursor bytes, and total request
  work.

The compiler derives these bounds from immutable contract and query inputs.
The caller supplies none of them. `within N` is a positive canonical literal
that cannot exceed 65,535 and is part of query identity, cost, role authority,
and generated documentation. It is a refusal ceiling, not a page: reaching
more than `N` distinct candidates fails the whole binding with its declared
typed outcome. No source or set operation truncates, samples, approximates, or
continues with a partial population.

Sources normalize their keys once. Set operations use canonical key equality,
deduplicate deterministically, and produce a canonical ascending internal
order solely for stable execution and hashing. That order has no result
semantics. A later root binding supplies the only observable result order.

### 2. Complete candidates before root order and page selection

The root binding consumes a candidate binding only through equality membership
on the complete declared root key. It point-hydrates or uses a compiler-proved
cover for every candidate at the same authorized snapshot, applies every root
predicate and row policy, derives the complete compiled sort key including its
unique tie-breaker, and sorts the complete surviving candidate population
before `take`, continuation probing, or cursor formation.

An intermediate candidate limit is never a result limit. The executor cannot
stop because it has enough rows for the requested page: an unseen candidate
may sort before every observed row. It either completes every source and set
operation inside the compiled ceilings or releases no result.

The candidate pipeline is same-partition. Every ordinary source, relationship
mapping, provider participant, root key, and hydration must derive the same
compiler-proved partition route. Cross-partition composition, arbitrary joins,
Cartesian products, recursion, runtime join ordering, adaptive provider
choice, population scans of authoritative entities, and application callbacks
remain forbidden.

Ordinary indexes and providers retain their own physical state and cost
models. Their only bridge is a typed canonical root-key batch plus a checked
participant observation under ADR-0130 and ADR-0164. A query combining provider
sources captures one admission head, selects generations sufficient for that
head, and binds the complete ordered participant identity into the plan and
cursor. It never combines observations from incompatible heads or silently
falls back to a stale generation.

Policy applies before observable membership, cardinality, sorting, and release.
Unauthorized rows influence no candidate count reported to the caller, sort
position, cursor, pattern diagnostic, or provider statistic. Internal overflow
and lifecycle failures use closed redacted resource identities and do not
reveal source-specific counts or values.

### 3. Keep caller-selected order finite and compiled

A source query may define a finite order family. The compiler expands that
family into immutable named plan variants, each with a fixed field sequence,
direction, null placement, physical encoding, and unique tie-breaker. Generated
facades may expose a closed enum selecting among those variants only when the
contract declares it; the service resolves the enum to an already compiled
plan identity before storage work.

No public request carries a field name, arbitrary order list, expression, index
hint, collation, or tie-breaker. The MLflow Experiment profile therefore needs
only compiled variants over `name`, `experiment_id`, `creation_time`, and
`last_update_time`, not a runtime sort language. Run-search order requires its
own complete inventory before its family is accepted.

Changing the chosen order changes cursor-family identity. Within one order
variant, ADR-0159 continues to exclude only the submitted result cardinality
from invariant parameter identity; predicates, candidate sources and ceilings,
provider participants, authorization, snapshot, and total order remain bound.

### 4. Add `long_pattern_v1` without widening storage keys

Contracts may declare one `long_pattern_v1` exact-pattern provider on a
bounded string field. Its declaration freezes:

- `binary_utf8_v1` or `unicode_fold_v1` matching;
- source-value and matched-value byte maxima;
- rows and total retained matched bytes per partition;
- distinct gram, postings, postings-byte, and per-row gram maxima;
- query-pattern bytes, wildcard atoms, literal runs, candidates,
  verification bytes, and result-window maxima;
- replay, checkpoint, generation-retention, and staleness bounds; and
- supported operator families selected from equals, starts-with, ends-with,
  contains, LIKE, ILIKE, NOT LIKE, and NOT ILIKE.

The maximum matched-value bound is the checked source maximum multiplied by
the selected profile's frozen expansion factor. V1 admits source values through
8,000 bytes and matched values through 144,000 bytes. These bytes are provider
values, never operational key components, so `MAX_KEY_BYTES` remains 4,096 and
every existing index key remains byte-exact.

Provider-state V1 retains each matched value once under its canonical entity
key, its collision-resistant digest, an ordered distinct three-byte UTF-8 gram
set, postings, and enough compiler-shaped output or authoritative locator data
to release the declared result. Equality may use the digest as a candidate
filter but must verify complete matched bytes. Other operators select
candidates from mandatory literal grams when available and then verify the
complete pattern against the retained matched value. Postings are an
acceleration structure, never semantic proof; verification removes collisions
and gram false positives.

Pattern semantics are frozen:

- binary matching compares valid canonical UTF-8 bytes;
- folded matching applies `unicode_fold_v1` to both value and pattern literal
  segments before pattern execution;
- `%` matches zero or more Unicode scalar values and `_` matches exactly one
  Unicode scalar value in the matched form;
- backslash escapes exactly `%`, `_`, and backslash; a trailing or other
  invalid escape is a source/input refusal;
- starts-with, ends-with, and contains retain ADR-0131's literal semantics and
  do not interpret wildcard characters;
- required fields have no null state; optional-field null/existence behavior
  remains the presence-aware predicate family and is not encoded as text; and
- matching is exact, deterministic, and independent of unrelated rows.

Patterns without a mandatory three-byte gram—including `%`, `_`, short
literals, and wildcard-only forms—use a compiler-declared bounded scan of the
provider's retained matched values. This is not an authoritative entity scan
and does not hydrate rows merely to decide text membership. It is still charged
against provider rows, matched bytes, pattern states, and verification work.
Exceeding any ceiling refuses the whole query. V1 never silently declares a
valid LIKE pattern unsupported merely because it lacks an accelerating gram.

The implementation must use a linear-space, bounded-work wildcard matcher; it
may not construct exponential backtracking state. Pattern atoms and matched
Unicode scalars are bounded before allocation. `NOT LIKE` and `NOT ILIKE` are
authorized set difference against a compiler-proved positive universe source,
not a database scan and not a complement inferred from unauthorized rows.

Existing exact-text V1/V2/V3 states, exact operators, checkpoint bytes, and
256-byte value ceiling remain unchanged. A deployment adopts
`long_pattern_v1` through a new declaration and rebuildable generation. The
tokenized provider remains unchanged and cannot be selected for pattern
semantics.

### 5. Raise the bounded result-row ceiling, not every other ceiling

The global query scan ceiling becomes 65,535 rows, reserving one continuation
probe and therefore admitting `Limit<MAX>` maxima and static `take` values
through 65,534. The maximum is deliberate: existing checked IR and service
boundaries represent page rows with `NonZeroU16`, it covers MLflow's 50,000-row
experiment page, and it is independently finite.

This amends ADR-0158 and ADR-0167 only where they state 499. A `Limit` parameter
still must declare its maximum, the default still does not narrow the domain,
and planner cost and role authority still use the declared maximum. Existing
compiled artifacts whose maximum is at most 499 retain their bytes and meaning.
New source above 499 selects the least-sufficient additive language, query IR,
module, plan, role, and generated-schema identities; old readers refuse them.

The larger row ceiling does not raise:

- the 4 MiB application-query encoded-result ceiling;
- unary request or response bytes;
- per-row, field, key, pattern, candidate, policy, hydration, provider,
  continuation, cursor, or diagnostic ceilings;
- MCP message, CLI output, or driver-frame byte ceilings; or
- a query family's compiler-declared candidate or result-window maximum.

Consequently a wide `Limit<50000>` query may correctly fail compilation because
its conservative maximum encoded result exceeds 4 MiB. A narrow projection may
compile and return tens of thousands of rows in one existing response. Where an
authoritative external API requires a larger exact logical page than one RiffDB
response can encode, ADR-0159's exception remains available: the owning adapter
may append successive RiffDB pages only in returned total order, under the
same immutable plan and fenced snapshot, up to the upstream request's explicit
finite maximum. It may not filter, sort, count, discard, restart, cross a
snapshot, or retain process-global continuation state. This is transport
assembly of an already correct result, not query semantics implemented in the
adapter.

The long-term extension point for results beyond the byte ceiling is a
first-party framed query stream that preserves this exact snapshot and cursor
contract. It requires separate real-consumer transport work and is not implied
by raising the row ceiling. An implementation package may not weaken the 4 MiB
boundary or add an SDK-only stream while claiming cross-surface parity.

### 6. Additive identities and compatibility

The first implementation assigns least-sufficient successor identities for:

- candidate-binding RiffQL, checked query IR, query module, executable plan,
  explain output, role authority, and generated operation schemas;
- page ceilings above 499;
- `long_pattern_v1` contract schema, bundle, provider descriptor, provider
  checkpoint, result-set participant, and pattern-query plan; and
- a finite compiled order-family selector if that selector reaches a public
  generated input.

Exact numeric tags are chosen only after WP-733 inventories every active and
reserved identity. No accepted tag is guessed or reused. A query or contract
without these features keeps its least-sufficient existing writer and byte
identity. Existing cursors, exact-text states, tokenized states, operational
keys, entity state, command formats, public Protobuf fields, and storage keys
are not reinterpreted.

Candidate and provider state is derived and rebuildable. Authoritative entity
state and the commit log remain the recovery source. Checkpoint publication is
whole-generation atomic; corruption, partial writes, identity mismatch,
excessive state, or an unavailable retained epoch fails typed before the
generation becomes query-visible.

## Options Considered

1. **Relax the dependent-batch order check.** Rejected. The intermediate
   `take` still truncates before final ordering, and point-batch execution does
   not prove a complete root population.
2. **Filter or sort in the adapter.** Rejected. It changes membership and
   pagination, crosses trust and snapshot boundaries, and duplicates database
   semantics outside first-party Rust.
3. **Copy root order fields into relationship rows.** Rejected. It creates
   unbounded denormalization, update amplification, and duplicated authority;
   it still does not solve arbitrary combinations of root predicates.
4. **Add general joins or a runtime optimizer.** Rejected. The consumer needs a
   finite same-partition semijoin over canonical keys, not application-authored
   join conditions, cross-partition execution, or cost-based plan choice.
5. **Raise `MAX_KEY_BYTES` or exact-text V1 limits.** Rejected. Unicode-folded
   8,000-byte values cannot fit a 4,096-byte key, and substring enumeration is
   quadratic. Widening a ubiquitous durable key format would not solve LIKE.
6. **Use tokenized search for LIKE/ILIKE.** Rejected. Word membership and exact
   wildcard matching answer different questions and cannot share an identity.
7. **Reject wildcard-only or short patterns.** Rejected for the accepted
   provider profile. A bounded provider-value verification path gives exact
   behavior without an authoritative entity scan.
8. **Remove all row ceilings.** Rejected. Application-facing interfaces remain
   statically bounded. Raising the structural ceiling to 65,534 while retaining
   query-specific rows, bytes, work, and authority is the safe capability.
9. **Raise the 4 MiB response ceiling together with rows.** Rejected. That is a
   transport, memory, and cross-surface decision with different evidence. The
   row ceiling should not force every response surface to allocate more bytes.
10. **Build a first-party streaming protocol in this package.** Deferred. It is
    the right future mechanism for very large byte results, but neither reported
    MLflow blocker requires changing all transports before correct search can
    land; ADR-0159 already permits exact bounded assembly at the owning adapter.

## Consequences

- Tag, relationship, exact-pattern, and tokenized membership can feed one
  complete bounded root result before caller-visible order and pagination.
- A candidate pipeline may materialize and sort up to 65,535 canonical keys in
  server memory. This is an explicit POC ceiling, independently byte- and
  work-bounded, and the algebra permits future spillable first-party
  implementations without changing public semantics.
- Long pattern search adds a potentially large derived structure and can do
  bounded linear provider verification for unselective patterns. Contracts see
  those costs before deployment; runtime never hides them behind truncation.
- Narrow queries may return over 499 rows. Wide queries remain constrained by
  encoded-result bytes and may require exact adapter assembly or future framed
  streaming.
- Provider/source intersections may retain more historical generation state
  so cursors can continue at their fenced snapshot. Retention remains finite,
  and expiry is typed.
- The implementation spans syntax, compiler, query IR/module, provider state,
  memory/redb execution, policy, service, generators, fixtures, handbook, and
  downstream compatibility. Staging is mandatory.

## Compatibility

This is additive except for increasing source-accepted `Limit<MAX>` and static
`take` maxima. Existing artifacts and queries keep exact meanings; a source
that previously failed solely because its bound was 500 through 65,534 may
compile only into successor identities. Existing readers never reinterpret an
old maximum or plan.

`long_pattern_v1` is a new rebuildable provider and checkpoint identity. No
existing exact-text or tokenized checkpoint changes. Candidate bindings and
order-family selectors use new query identities. No command, entity, index-key,
commit-log, outcome, event, or provenance encoding changes.

The external MLflow adapter and its schema remain outside this repository.
Only framework-neutral capability fixtures and value-free downstream receipts
are checked in here.

## Security

Candidate sources, set operations, and root hydration execute only after fresh
authorization and at one policy-aligned snapshot. The compiler derives union
authority over every possible source, root field, hydration, and output.
Selecting a different compiled order cannot widen row or field authority.

Negation uses an authorized positive universe and therefore cannot expose the
existence of denied rows. Unauthorized rows affect no count, overflow detail,
sort rank, cursor, provider statistic, or timing label. Public errors report
only closed resource identities, actual bounded work classes where safe, and
maxima; values, tag keys, patterns, candidate keys, and provider internals are
redacted.

Adversarial patterns are bounded by input bytes, wildcard atoms, scalar count,
candidate rows, verification bytes, and linear matcher work. No regex,
backtracking engine, locale selector, collation selector, runtime provider,
cost hint, fallback, or consistency downgrade is exposed.

The higher row ceiling does not grant authority implicitly. Every named query
declares its own `Limit<MAX>`, planner and role costs use that maximum, and the
4 MiB result ceiling remains fail-closed.

## Standing Design Tests

- **Interface safety:** Applications invoke generated named operations with
  typed predicate values, a closed order selector if declared, a bounded limit,
  and opaque cursor. They cannot submit sources, keys, joins, set algebra,
  indexes, providers, fields, operators, patterns outside the compiled family,
  costs, policy placement, snapshot selection, or fallback. Every overflow is
  whole-operation refusal, so no surface can silently trade correctness for a
  partial result.
- **Scale:** V1 materializes at most 65,535 candidate keys in one partition and
  may linearly verify at most the declared provider rows/bytes for an
  unselective pattern. This is a deliberate reversible POC implementation
  constraint, not an assumption that the authoritative database fits in
  memory. Candidate-source, provider, sort, hydration, output, and retained
  epoch bounds are independent. The public algebra can later use partitioned
  spill or maintained set structures without changing result semantics.

## Testing

The staged acceptance corpus must include:

- parser and source-span snapshots for every candidate source, set shape,
  bound, type, partition, order, provider, pattern, and page-limit refusal;
- checked-IR/module/plan canonical fixtures plus old-artifact byte equality;
- an independent set-and-sort oracle proving filters complete before root
  order, limit, and cursor selection, including the two-tag reproduction;
- memory/redb parity for intersection, union, difference, duplicates, missing
  targets, empty sources, maximum candidates, overflows, cancellation, and
  forward/reverse cursors;
- authorization adversaries proving denied rows affect no membership, negation,
  overflow, order, cursor, or diagnostic;
- binary and Unicode-fold pattern corpora covering empty values, composed and
  decomposed text, fold expansion, `%`, `_`, escaping, short literals,
  wildcard-only patterns, collisions, and maximum 8,000-byte values;
- property comparison to an independent exact LIKE evaluator and randomized
  mutation/delete/rebuild/compaction/recovery histories;
- process-crash tests proving no partial provider generation or frontier
  publishes;
- boundary tests at page rows 499, 500, 50,000, 65,534, and 65,535, plus
  encoded-result and role-cost refusals; and
- generated Rust, Go, TypeScript, Python, gRPC, MCP, CLI, local/remote,
  downstream-adapter, handbook, topology, and compatibility checks.

## Requirements and Work Packages

If accepted, register a closed requirement family and these packages:

- **WP-733:** MLflow-neutral search compatibility inventory, exact syntax,
  identity inventory, and failing fixtures.
- **WP-734:** candidate binding syntax, checked IR/module identity, compiler
  proofs, reference executor, policy, and ordinary memory/redb sources.
- **WP-735:** provider participants, complete root hydration/sort, cursor and
  admission-head fencing, generated order families, and cross-surface service
  execution.
- **WP-736:** 65,534-row ceiling through syntax, cost, role, runtime, storage,
  public messages, generators, compatibility, and documentation.
- **WP-737:** `long_pattern_v1` declaration, durable provider, wildcard
  evaluator, rebuild/recovery, and candidate-source integration.
- **WP-738:** combined generic corpus, four-language conformance, downstream
  receipts, handbook closure, and full workspace acceptance.

WP-734 depends on WP-733. WP-735 depends on WP-734. WP-736 depends on WP-733
and may proceed in parallel with WP-734 after exact acceptance. WP-737 depends
on WP-733 and WP-734. WP-738 depends on WP-735, WP-736, and WP-737.

## Decision Deadline

The exact text must be accepted before WP-734 changes RiffQL, query IR, module,
plan, cursor, authority, or execution semantics; before WP-736 raises a public
row ceiling; and before WP-737 freezes a provider or checkpoint identity.
