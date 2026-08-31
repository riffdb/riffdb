# ADR-0173: Tokenized Text Search Provider

- **Status:** Proposed
- **Direction approved:** 2026-08-30
- **Exact text accepted:** No
- **Accepted:** Not accepted
- **Acceptance reference:** Maintainer asked for the ability to project and
  query a search store in the current Claude Code session; the exact text is
  not yet accepted
- **Decision deadline:** Before an application ships a second store alongside
  RiffDB to answer text queries
- **Requires:** ADR-0131, ADR-0164, ADR-0172
- **Amends:** Nothing; adds a provider alongside the exact-text provider
- **Defines or blocks:** Nothing yet

## Context

RiffDB's text surface is deliberately closed. `ExactTextOperatorV1` carries
exactly `Equals`, `StartsWith`, `EndsWith`, and `Contains`, and its own comment
states the boundary: "no wildcard, regex, token, fuzzy, or score shape exists."

That closure is correct for what it is. It also means there is no answer to the
ordinary request behind most search features — find rows matching several terms
in any order and return the best ones first. An application needing that has to
run a second store beside RiffDB, which reintroduces exactly the dual-write,
dual-consistency, dual-authorization problem RiffDB exists to remove.

The product direction is a one-stop store for multi-tenant applications, with
opinionated native analytics, vectors, and text search. Vectors already landed
under that model: `vector_field` declares source fields, an embedding model
identity and version, a staleness SLO, replay bounds, and an optional
approximate clause with `ann_threshold` and `recall_target_bps`. Search is
exact at or below the threshold, approximate above it, and the caller cannot
weaken the declared recall target.

That precedent is the shape this record follows. What is missing is not a query
syntax; it is a provider.

## Evidence

Two adapters have now needed it. An MLflow tracking store requires `ILIKE` over
tag values and names with SQL wildcard semantics, and its `search_runs` and
`search_experiments` surfaces are conjunctions of such predicates. A
better-auth adapter needs user lookup by partial name and email.

Both are currently served, if at all, by `contains` over a single field, which
is a substring test with no tokenization, no multi-term conjunction, no
relevance, and no defence against a needle that matches most of the partition.

## Decision

Add a tokenized text-search provider, declared in the contract and queried
through named operations, following `vector_field`'s declaration model.

### Declaration

A `text_index` declares its source fields with per-field weights, an analyzer
from a closed set, a staleness SLO, replay bounds, and its result model — the
same obligations `vector_field` carries, for the same reasons.

The analyzer vocabulary is closed and opinionated, not open:

- `keyword_v1` — no tokenization. The whole value is one term. This is what a
  tag value, identifier, or enum-like string wants; tokenizing it produces
  matches nobody asked for.
- `standard_v1` — Unicode word segmentation followed by ADR-0172's fold. This
  is what prose wants.

Stemming and language packs are deliberately excluded from the first
vocabulary. Each language is pinned data carrying the same upgrade-freezing
problem as the Unicode tables, and stemming is lossy in ways users notice and
report as defects. A future `standard_en_v1` is a new analyzer, added when a
real application needs it, never a change to `standard_v1`.

Analyzers are versioned exactly as an embedding model is. Tokenization,
normalization, and stop handling are a frozen function; changing them changes
what the index contains and is a new analyzer version with a rebuild, never an
in-place upgrade.

### The segment is durable, rebuildable, and versioned

The provider owns a durable inverted index with a versioned checkpoint format,
following the exact-text provider rather than the vector provider.

The vector provider rebuilds its graph ephemerally per query, and
`docs/known-limitations.md` records what that costs: "this POC does not claim
persistent-graph latency for large partitions." Rebuilding an inverted index
per query is work proportional to the partition, which defeats the bound a
provider exists to give. Text corpora are exactly where that hurts.

**The format carries postings, term frequencies, length norms, and token
positions from its first version.** Frequencies and norms are what ranking
needs; positions are what phrase and proximity need. All four are committed up
front even though the first release ships less, because adding any of them
later is a segment-version change and a rebuild of every deployed index — and a
rebuild is exactly the migration an operator cannot do casually.

### Matching is boolean and exact; ranking is an ordering over the match set

This is the load-bearing decision, and it is where a search provider usually
goes wrong inside a database with exact pagination.

**Matching** is a bounded boolean expression over analyzed terms —
conjunction, compiler-capped disjunction, phrase, and bounded proximity. It is
exact and deterministic: a row either contains the terms in the required
arrangement or it does not, and that answer does not depend on what else is in
the partition.

**Ranking** is a total order over the matched set. It is not part of the match
semantics, and it never changes membership.

The separation matters because the obvious ranking function, BM25, is a
function of corpus statistics — document frequency and average length across
the partition. A score therefore changes when unrelated rows are written, which
makes a naive scored cursor unstable: page two under a changed corpus is not
the continuation of page one.

RiffDB already has the mechanism for this. ADR-0164 fences a query to one
authorized application head and freezes that floor across cursors. A ranked
search binds its corpus statistics to the same fenced snapshot, so a paginated
ranked result is a total order over one frozen corpus. A cursor that outlives
its snapshot fails with the existing typed lifecycle outcome rather than
silently reordering.

### Relevance is provider-owned; emphasis is compile-time

The scoring function is frozen and not selectable, like `recall_target_bps`.
The contract declares **per-field weights**, and nothing else.

Weights exist because without them a title match ranks identically to a body
match, which is wrong for nearly every application that has both. They are
compile-time and part of index identity, so relevance stays a property of the
deployed contract. Callers submit no boosts, no scoring parameters, and no
field emphasis; an application wanting a different notion of best declares its
own order fields and takes the match set unranked.

### Bounded like everything else

The provider declares, and the compiler charges, a maximum term count per
query, a maximum candidate set before ranking, and a maximum result window.
Exceeding a budget is a typed refusal, never a truncated page or a partial
result. Partition scoping is unchanged: one query, one partition.

### What it is not

Not a general query language. Callers invoke named generated operations and
never submit fields, operators, analyzers, boosts, or scoring parameters — the
same rule the exact-predicate families already state.

Not a relevance-tuning surface. The scoring function is frozen and
provider-owned, like `recall_target_bps`. Per-field weights are declared in the
contract and are the only emphasis available; an application that needs a
different notion of best supplies its own order fields instead.

Not a replacement for the exact-text provider. `starts_with` on an unanalyzed
value is a different question from a term match, both are legitimate, and a
tokenized index cannot answer prefix questions about the raw value.

## Options Considered

**Extend the exact-text provider with token operators.** Rejected. Its closure
is a stated property, its checkpoint format is built around whole-value
matching, and tokenization changes the row-to-entry cardinality from one-to-one
to one-to-many. That is a different structure wearing the same name.

**Ship boolean matching and no ranking.** Seriously considered, and it is the
honest minimum: it is exact, cursor-stable without a fence, and much smaller.
Rejected as the destination because "find the best matches" is the actual
request, and an application that has to rank client-side must first fetch every
match, which defeats the bound. Retained as the first implementation stage —
the match semantics are the foundation and are useful before ranking exists.

**Rebuild the index ephemerally per query.** This is what the vector provider
does, and it would remove the durable format, its compatibility rules, and its
rebuild and recovery paths. Rejected: the cost is already documented for
vectors as a POC limitation on large partitions, and an inverted index rebuilt
per query is work proportional to the partition — the opposite of what a
bounded provider promises. A hybrid on a declared threshold was also
considered and rejected as two execution paths that must agree exactly on
results, for a saving only small partitions see.

**Commit a smaller segment format now and extend it later.** Rejected. Postings
alone are the cheapest thing to ship and to verify, but adding frequencies,
norms, or positions afterwards is a segment-version change requiring a rebuild
of every deployed index. Operators do not rebuild casually, and a format that
is known to be insufficient should not be released. Positions in particular are
committed even though phrase is not the first feature shipped, because they are
the most expensive thing to retrofit.

**Let callers supply per-field boosts.** Rejected. It is the familiar
Elasticsearch surface, and it makes relevance a caller-submitted query
parameter, contradicting the rule that callers never submit fields, operators,
cost hints, or provider choices. Compile-time weights give applications the
emphasis they actually need without opening that surface.

**One frozen analyzer with no choice.** Rejected. It forces tokenization onto
tag values, identifiers, and enum-like strings, where whole-value matching is
the correct behaviour and tokenization produces matches nobody asked for. Two
analyzers is the smallest vocabulary that serves both of the applications
driving this record.

**Adopt an embedded search library.** Rejected on the same grounds ADR-0172
rejects a system Unicode library: index contents would become a function of a
dependency's version, and the segment format, analyzer, and scoring would be
owned outside the contract's compatibility rules. The provider must own its
durable format.

**Direct applications to vector search instead.** Rejected. Semantic similarity
does not answer a lexical query; a user searching for an exact error code or a
tag value wants that token, not its neighbourhood. The two are complementary,
and an application will reasonably want both over the same fields.

## Consequences

An application can keep text search inside the same store, snapshot,
authorization boundary, and backup as its authoritative data. That is the whole
point of the direction.

The cost is a second derived structure per declared index, with its own
rebuild, replay, staleness, and recovery paths. Every one of those already
exists for vectors and for exact text; this adds a third participant to them
rather than inventing new machinery, and that is the main argument for the
shape.

Positional postings are materially larger than frequency-only postings, and for
long prose fields the positions can dominate the segment. The compiler must
charge that, and the declaration's bounds must make it visible before a
contract deploys rather than after an operator notices the disk. The exact
multiple depends on the posting encoding and is a matter for the
implementation, not for this record.

The ranking fence has a visible consequence: a long-lived ranked cursor can
expire where an unranked one would not, because it is pinned to corpus
statistics as well as to the application head. That must be documented as a
property of ranked search rather than discovered as a defect.

Analyzer changes require rebuilds, and analyzers change more often than people
expect — a stop-word or stemming adjustment is an analyzer change. The version
must be in the declaration so the rebuild is forced rather than forgotten.

## Compatibility

Additive. No existing durable format, contract, plan hash, or provider changes.
A contract without a `text_index` is byte-identical. The provider's own
checkpoint format is versioned from its first release.

## Security

The provider inherits partition scoping and capability authorization
unchanged; a text query cannot read outside the partitions its capability
names. Two properties need explicit attention:

- Scoring must not become an oracle. Document-frequency statistics summarise
  the partition, and a caller authorized for the partition may already observe
  them; a caller whose capability is narrower than the partition must not.
  Corpus statistics are therefore scoped to the authorized set, not to the
  physical partition.
- Term-count and candidate budgets are the defence against an adversarial
  query. They are compiler-declared, not caller-supplied.

## Standing Design Tests

- Match membership is independent of corpus contents; adding an unrelated row
  never changes whether a matching row matches.
- A ranked page sequence over one fenced snapshot is a total order with no
  duplicate and no omission.
- A ranked cursor whose snapshot retired fails with the existing typed outcome
  rather than reordering.
- An analyzer version change produces a different index identity, and so does
  a field-weight change.
- `keyword_v1` produces exactly one term per value, whatever the value
  contains.
- A phrase match requires adjacency, and a proximity match requires the
  declared distance; neither is satisfied by a row containing the terms in the
  wrong arrangement.
- Exceeding the term, candidate, or result budget is a typed refusal, never a
  truncated result.

## Testing

Boolean match semantics against a reference implementation over a fixed corpus;
pagination exactness under concurrent writes; budget refusals; and an analyzer
conformance corpus pinned like ADR-0172's fold corpus.

## Requirements and Work Packages

Needs staged work packages. The segment format is decided once and up front —
postings, frequencies, norms, and positions — because it is the part that
cannot be revised without a rebuild. Implementation then stages: the
declaration and the two analyzers; the durable segment with its rebuild,
replay, and recovery paths; boolean matching including phrase and proximity;
and ranking over the fenced corpus with declared field weights.

Boolean matching is independently shippable and should ship first, so the
ranking work is done against a working index rather than on paper. The format
commitment is what makes that staging safe.

## Decision Deadline

Before an application ships a second store beside RiffDB to answer text
queries. Once a dual-write path exists it tends to stay, and the consistency
argument for a single store is much harder to make after the fact.
