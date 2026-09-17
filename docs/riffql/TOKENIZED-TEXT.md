# Tokenized text search

RiffDB tokenized search is a finite family of compiled named queries over a
declared `text_index`. It is separate from exact-text `starts_with`,
`ends_with`, and `contains`: tokenized search matches analyzed terms, while
exact text compares the stored value's bytes.

## Declare the index

One entity declaration names the indexed fields, their positive integer
weights, one analyzer, provider lifecycle bounds, and query budgets:

```riff
text_index search(
  (title weight 4, body weight 1),
  analyzer standard_v1,
  staleness_slo 60,
  replay_age_seconds 86400,
  replay_bytes 1073741824,
  replay_backlog 100000,
  result boolean_v1,
  max_terms 16,
  max_candidates 10000,
  max_results 1000
)
```

`keyword_v1` preserves the complete value as one term. It is intended for tags,
identifiers, and enum-like strings. `standard_v1` applies Unicode 17 word
segmentation and then the pinned Unicode fold. Neither analyzer stems words or
uses a language-specific stop-word list. Analyzer identity and field weights
are part of the contract and provider identity; changing either rebuilds the
derived index.

The three query budgets are whole-operation ceilings. An empty analyzed query,
too many terms, too many candidates, or too many results returns a typed
refusal. RiffDB never truncates an oversized match set and never falls back to
an entity scan.

## Compile a named operation

RiffQL V10 has exactly four match shapes. The source chooses the index, shape,
and—only for proximity—the positive distance:

```riffql
query search_docs(
    $org: Document.organization_id,
    $query: Document.title,
    $limit: Limit<20>,
    $cursor: Cursor,
) {
    many docs from Document where docs.organization_id == $org
        matching(search, conjunction, $query, riff_bm25_v1)
        order by docs.doc_id asc
        take $limit after $cursor
    return { doc_id: docs.doc_id title: docs.title }
}
```

Replace `conjunction` with `disjunction`, `phrase`, or `proximity, 5` to compile
the other shapes. Conjunction requires every analyzed term. Disjunction requires
at least one. Phrase requires consecutive positions in one indexed field.
Proximity requires the terms in order in one indexed field, with each adjacent
distance no greater than the compiled bound. Repeated intermediate terms are
considered at every reachable position: an earlier occurrence cannot hide a
later occurrence that completes the match.

Omitting `riff_bm25_v1` gives Boolean execution and canonical entity-key order.
Adding it selects the only ranked order. Applications cannot submit a field,
analyzer, shape, distance, weight, score function, boost, tie-breaker, provider,
budget, cost hint, scan, or fallback at runtime. Generated Rust, Go,
TypeScript, Python, gRPC, CLI, and MCP operations expose only the query's typed
values and the ordinary opaque cursor/options carriage. Scores are not public
result fields.

## Deterministic ranking

Ranking never changes membership. It orders the exact Boolean match set over
the caller's complete authorized corpus at one admission-head-fenced snapshot.
Unauthorized rows contribute to neither results nor document-frequency and
field-length statistics.

`riff_bm25_v1` uses checked unsigned integer arithmetic and scale
`S = 1,000,000`. For authorized document count `N`, term document frequency
`df`, term frequency `tf`, document field length `dl`, total authorized token
length `L` for the field, and its contract weight:

```text
idf_scaled = floor(S * (N - df + 1) / (df + 1))
denominator = tf*S*L + 1200*(250*L + 750*dl*N)
tf_scaled = floor(S * tf * 2200 * 1000 * L / denominator)
term_contribution = floor(weight * idf_scaled * tf_scaled / S)
score = checked_sum(term_contribution)
```

Every division rounds toward zero. Equal scores use canonical entity-key bytes
ascending. Arithmetic that cannot fit the frozen checked representation is a
typed whole-result refusal, not saturation.

## Freshness, rebuilds, and cursors

The production request path uses only a ready, policy-aligned posting segment.
A background worker builds and checkpoints it from one bounded authoritative
snapshot. Initial build, rebuild, capacity exhaustion, stale state, corruption,
and an unavailable epoch are typed provider lifecycle outcomes. There is no
request-time authoritative scan or stale-generation fallback.

A ranked first page captures admission-head consistency and binds its causal
floor, provider epoch and generation, authorized statistics identity, immutable
plan, invariant parameters, current capability identity and revision, history
incarnation, total order, and result ceiling to the opaque cursor. Continuations
reuse that snapshot and never recapture statistics. The production POC retains
eight epochs per registered query shape. If the bound epoch is retired—or the
cursor expires, authority changes, or history is restored—the continuation
fails typed rather than rescoring against newer data.

Boolean results do not expose a tokenized continuation. They use the compiled
bounded numeric offset and canonical entity-key order; separate requests do not
freeze the corpus across writes. Ranked pages use the ordinary generated cursor
option and may request a different valid page size without changing the frozen
operation identity.

## Deliberate exclusions

V1 has no wildcard, regular expression, fuzzy matching, stemming, language
packs, highlighting, snippets, caller-selected relevance controls, score
output, hybrid vector/text ranking, general joins, or ad hoc search endpoint.
Applications export their changelog or authoritative data when a different
search product is the better fit.

Ranked execution computes each distinct query term's document frequency and
each field's corpus length once for the captured provider snapshot. Scores and
statistics identity share those values. Repeated query terms still contribute
repeatedly to the frozen fixed-point score; they do not alter corpus identity.
