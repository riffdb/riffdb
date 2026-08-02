# ADR-0092: Native Full-Text Search as a Projection

- **Status:** Proposed
- **Date:** 2026-08-02
- **Decision owners:** RiffDB maintainers
- **Related:** ADR-0086 (projection plane), ADR-0087 (ad-hoc grammar
  governance), ADR-0091 (vector search; shares the per-tenant
  corpus-statistics rule)

## Context

Full-text search is the second search pillar of the native-platform thesis
(ADR-0091 states the first). Unlike vector search, lexical search is exact:
given a tokenizer, a scoring function, and a corpus at a frontier, results
are deterministic. That means FTS fits ADR-0086's existing acceptance
contract with NO amendment — the interesting decisions are tokenization
governance and the inference channel hiding inside corpus statistics.

## Decision

### 1. An FTS index is a projection, under the unamended 0086 contract

An inverted index over declared text fields is a projection in full:
derived, org-partitioned, frontier-reporting, freshness-classed, typed
lifecycle outcomes, replay-budgeted, snapshot-rebuildable (source text lives
in authoritative entity state). Because scoring is deterministic, the
reference-evaluator acceptance applies EXACTLY: a naive scan-and-score
evaluator at the same frontier must produce identical results, ranks
included, and compaction is result-invariant in the strict sense.

### 2. Corpus statistics are per-organization (the inference rule)

Relevance scoring uses corpus-wide statistics (document frequencies, field
length norms). Computed globally, they are an inference channel: a score can
reveal the existence of documents the principal cannot see. Therefore all
scoring statistics are computed **within one organization's partition**, and
row-level policy applies **before scoring and before snippet extraction** —
scores, ranks, highlights, and result counts derive only from rows the
principal may see, extending ADR-0086 §5's inference protection to lexical
ranking. (Statistics per *authorization scope* below the organization are
explicitly NOT attempted in v1: the residual channel — scores shaped by
same-org documents outside the principal's row policy — is stated policy,
mirroring 0086 §5's "beyond stated policy" boundary.)

### 3. Analyzers are declared, versioned contract surface

Tokenization is declared per text field (analyzer name + version + language
where applicable) in the projection definition. Analyzer output feeds the
durable index, so an analyzer change is a declared migration that rebuilds
the projection with typed `Rebuilding` progress — never an in-place
reinterpretation. v1 ships an opinionated small analyzer set (simple,
language-default stemming, exact/keyword); custom analyzers are deferred.

### 4. Grammar

New predicates entering the 0087 surface by this amendment: `match(field,
$query)`, `phrase(field, $query)`, and `prefix(field, $term)`, plus ordering
by relevance score — all composable with existing equality/range predicates
and subject to required limits and scan budgets. Deferred, each a future
amendment: fuzzy matching, per-field scoring profiles, highlighting beyond
basic snippets, and hybrid fusion with ADR-0091's nearest-neighbor results.

## Rejected alternatives

- **External search engine sidecar** (the classic bolt-on): two systems of
  record, replication lag invisible to the freshness contract, and the
  authorization model enforced twice in two codebases — the wiring this
  database absorbs.
- **Global corpus statistics with post-hoc filtering.** Leaks existence
  through scores; filtering after ranking is the classic mistake §2 forbids.
- **SQL-style LIKE scans as the v1 story.** Unbounded scans on the
  authoritative path violate ADR-0086's rejected-alternatives list already.

## Acceptance criteria (feasibility prototype + evidence)

Byte-exact result and rank equality against the reference evaluator at
matched frontiers across randomized histories, including deletes/updates of
indexed text; determinism across restarts and compactions; per-organization
statistics isolation verified adversarially (a document in org A never
influences scores, ranks, counts, or timing in org B beyond stated policy);
row policy before scoring and snippets verified (unauthorized rows influence
nothing); analyzer-version migration rebuilds completely with typed
progress; all ADR-0086 frontier/crash invariants hold unchanged.

## Acceptance

Pending maintainer decision.
