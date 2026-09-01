# Long-value pattern search

`long_pattern_v1` is RiffDB's exact, bounded pattern facility for strings that
are too large for ordinary operational text keys. It participates in a named
RiffQL candidate binding before final root authorization, ordering, limit, and
cursor selection. It is not tokenized search, SQL, a regular-expression API, or
a request-time fallback scan.

## Declare the provider

A contract attaches one provider to one bounded string field and freezes every
operator, matching profile, lifecycle setting, and resource ceiling:

```riff
pattern_index by_name_pattern(
  name,
  profile unicode_fold_v1,
  operators (equals, starts_with, ends_with, contains, like, ilike,
    not_like, not_ilike),
  max_source_bytes 8000,
  max_matched_bytes 144000,
  max_rows 65535,
  max_total_matched_bytes 268435456,
  max_grams_per_row 8000,
  max_distinct_grams 65535,
  max_postings 1048576,
  max_postings_bytes 67108864,
  max_pattern_bytes 8000,
  max_pattern_atoms 8000,
  max_candidates 65535,
  max_verification_bytes 268435456,
  max_results 65534,
  staleness_slo 60,
  replay_age_seconds 86400,
  replay_bytes 1073741824,
  replay_backlog 100000,
  retained_generations 8
)
```

All values are checked compiler input and provider identity, not caller cost
controls. Source strings may be at most 8,000 UTF-8 bytes. Unicode folding can
expand the retained matched form, so its independent maximum may be as high as
144,000 bytes. The provider does not widen RiffDB's 4,096-byte storage-key
ceiling and does not copy quadratic substring keys.

## Use it before root ordering

The provider is named only in compiled query source. Generated clients submit
the typed pattern, ordinary invariant parameters, page limit, and opaque cursor;
they cannot choose a provider, gram, matcher, scan, sort field, or budget.

Declare the pattern parameter with the provider field's type, for example
`$name_pattern: Experiment.name`. An explicit bounded string with the same
bound, such as `$name_pattern: string<500>`, is equivalent. RiffDB resolves
both forms to the exact contract value type and rejects an unknown field or a
mismatched bound at the parameter or predicate source span.

```riffql
candidates matching_experiments: Experiment.experiment_id
    from intersect {
        ExperimentTag.experiment_id using by_tag_digest
            where scope == $scope
                && tag_key == $tag_key
                && value_digest == $tag_digest,
        Experiment.experiment_id using by_name_pattern
            where scope == $scope
                && name ilike $name_pattern,
    }
    within 65535
    else IntegrityFailure

many experiments from Experiment
    where scope == $scope
        && experiment_id in matching_experiments
    order by last_update_time desc, experiment_id asc
    take $limit after $after
    else IntegrityFailure
```

Every source completes and deduplicates before the set intersection. RiffDB
then hydrates the complete root candidate population in one authoritative
snapshot, reapplies root policy, derives every total-order key, and sorts before
selecting the page or minting a cursor. It never filters a pre-paginated root
page or stops when it has found enough rows for the requested limit.

## Exact matching semantics

`binary_utf8_v1` compares canonical UTF-8 bytes. `unicode_fold_v1` applies the
pinned Unicode fold before matching. Equality and literal `starts_with`,
`ends_with`, and `contains` do not interpret wildcard characters.

For `like` and `ilike`, `%` matches zero or more matched-form Unicode scalar
values and `_` matches exactly one. Backslash escapes only `%`, `_`, and
backslash; an incomplete or different escape is refused. Matching uses bounded
linear-space state.

Digests and mandatory three-byte literal grams are candidate filters only.
Every candidate's complete retained matched value is verified before membership,
so hash collisions and gram false positives cannot alter results. A short or
wildcard-only pattern with no mandatory gram performs a compiler-bounded scan
of retained provider values, never an authoritative entity scan.

Negated membership is expressed as candidate `difference` from a complete,
policy-authorized positive universe. A direct `not_like` or `not_ilike`
provider source is refused because complementing only provider-visible rows
could reveal hidden membership.

## Lifecycle and bounds

Background workers build policy-aligned provider state from one exact
authoritative snapshot and atomically activate a checksummed generation. A
request uses only a ready generation whose descriptor, schema, policy shape,
plan, history incarnation, frontier, and work observation match the compiled
query. On first use or catch-up, RiffDB registers every compiler-bounded
provider participant together and waits within the request deadline and its
finite server-owned readiness ceiling for the background worker to publish a
sufficient generation. Current authority is revalidated after every wait.
Callers cannot select the worker, cadence, ceiling, provider, or fallback, and
the request path never performs the rebuild itself.

If building, rebuilding, or freshness work does not finish inside that bound,
or if capacity exhaustion, corruption, stale state, or generation retirement
is observed, the query returns the corresponding typed refusal. It never
reports provider activation as generic storage unavailability and never serves
a lower frontier or partial candidate population.

The provider and ordinary candidate sources share one admission-head-fenced
participant proof. A cursor binds that participant set and the complete root
order. If its authoritative snapshot or provider generation can no longer be
served, continuation fails typed instead of switching to current state.

Candidate, verification, result, replay, checkpoint, retention, root hydration,
policy, sort, and output work are independent. The structural page maximum is
65,534 rows with one reserved continuation probe, but the compiled query's own
maximum and the unchanged 4 MiB encoded-result and transport ceilings can make
the practical page smaller for wide rows.
