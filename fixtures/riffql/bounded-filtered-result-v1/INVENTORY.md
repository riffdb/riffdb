# Bounded filtered-result inventory

This is WP-733's implementation inventory for accepted ADR-0174. It freezes
the inputs required by WP-734 through WP-738; it does not claim that candidate
syntax, larger pages, or `long_pattern_v1` execute at this checkpoint.

## Upstream revision and allowed scope

The inventory was prepared from `ce4bde51`, where ADR-0174, `OQ-084` through
`OQ-100`, and WP-733 through WP-738 are authoritative. WP-690's operational
access corpus and WP-719's admission-head implementation are present on main at
`2f3b93c2` and `5ce580ed`; WP-731's tokenized-query closure is recorded in the
manifest. WP-733 changes only its declared documentation, fixture, script,
topology-inventory, ADR, SPEC, and work-package paths.

## Real-consumer compatibility matrix

The retained evidence is framework-neutral. `experiment` means a partitioned
root row, and `tag` means a relationship row projecting that root's exact key.

| Surface | Required semantics | Existing capability | ADR-0174 destination |
|---|---|---|---|
| Two or more tags | Complete AND before root order | Dependent batches require join-key order and page intermediates | Candidate intersection, then root hydration/order/page |
| Tag absence or inequality | Authorized complement before root order | No positive-universe candidate difference | Candidate difference from a compiled authorized universe |
| Root caller order | Four finite attribute choices with directions and stable tie-break | One order is compiled per named query | Finite compiled order family; no runtime field names |
| Name pattern | Exact binary or folded prefix/suffix/substring/LIKE | Exact provider is limited to 256 matched bytes | `long_pattern_v1` candidate source plus exact verification |
| Tag-value pattern | Exact matching through 5,000 or 8,000 source bytes | Folded operational key may exceed 4,096 bytes | Provider-retained matched value through 144,000 bytes |
| `%` and `_` | SQL-shaped wildcard semantics including unselective patterns | Deliberately unsupported | Bounded provider-value verification with no authoritative scan |
| Requested page | Up to 50,000 root rows | Structural result ceiling 499 | Structural result ceiling 65,534; independent 4 MiB result bytes |
| Oversized encoded page | Same ordered logical page | ADR-0159 exact append is already accepted | Same fenced cursor chain; no filtering, sorting, or restart |

## Frozen source vocabulary

The candidate corpus in
`fixtures/riffql/bounded-filtered-result-v1/queries` freezes these spellings:

```riffql
candidates NAME: Root.key_field
    from SOURCE
    within POSITIVE_LITERAL
    else OUTCOME

from intersect { SOURCE, SOURCE }
from union { SOURCE, SOURCE }
from difference { POSITIVE_SOURCE; NEGATIVE_SOURCE, NEGATIVE_SOURCE }
```

One source has the form:

```riffql
Entity.projected_root_key using declared_access_name
    where compiler_checked_predicates
```

The source access name is contract text, not a request value. A root consumes a
candidate binding only as `complete_root_key in candidate_name`. Candidate
bindings have no `take`, cursor, output projection, or target-language type.
The maximum of eight sources and `within 65535` are structural maxima;
individual queries may declare smaller values.

The contract corpus freezes this declaration shape:

```riff
pattern_index NAME(
    FIELD,
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

Every number is checked compiler input and part of provider identity. The
implementation may reuse a common bounded-clause parser, but it may not infer
or omit one of these independent charges.

## Diagnostic allocation

Existing syntax codes `RDB-QS003`, `RDB-QS004`, `RDB-QS008`, and `RDB-QS009`
continue to report token, structural, unsupported, and missing-bound failures.
The candidate parser does not allocate a feature-specific syntax code.

- `RDB-QP011` is reserved for an invalid candidate source, algebra, root-key,
  partition, positive-universe, root-consumer, or completion/order proof.
- `RDB-QP012` is reserved for a long-pattern query whose declared operator,
  profile, field, participant, or bound is incompatible with the provider.
- `RDB-C048` is reserved for an invalid `pattern_index` declaration or a
  declaration whose field/profile/operator combination is inconsistent.
- Existing `RDB-C020` reports closed contract resource maxima; `RDB-QP010`
  continues to report whole-query cost or result-byte ceilings.

All diagnostics are source-spanned and value-free. Runtime candidate,
provider, freshness, cursor, and authorization failures use existing typed
query lifecycle/refusal families rather than allocating one error per source.

## Identity allocation

The inventory found no accepted or reserved collisions at `ce4bde51`.

| Domain | Active maximum | ADR-0174 allocation | Selection rule |
|---|---:|---:|---|
| RiffQL language | 10 | 11 | Candidate syntax or a limit maximum above 499 |
| Query IR | 13 | 14 | Candidate plan, long-pattern candidate query, or bound above 499 |
| Query-module codec | 13 | 14 | Embeds query IR V14 |
| Contract grammar | 20 | 21 | Contains `pattern_index` |
| Contract executable IR | 20 | 21 | Contains a checked pattern provider |
| Contract bundle | 20 | 21 | Contains contract grammar/IR V21 |
| Compiled application-role codec | 5 | 6 | Carries candidate/pattern/source/order authority atoms |
| Projection-provider descriptor | 1 | 1 plus a new provider-kind tag | Existing descriptor framing already closes kind and bounds; unknown tags refuse |
| Long-pattern provider checkpoint | new domain | 1 | First rebuildable provider-state format |
| Candidate result-set participant | existing participant framing | additive kind tag | Same fenced participant envelope; unknown tags refuse |
| Cursor wire/token | unchanged | unchanged | Process-local registry binds a new plan and participant identity |
| Generated order selector | new closed schema member | 1 | Present only for a declared finite order family |

V11/V14/V14 is a common least-sufficient family: a high-limit-only query does
not also gain candidate nodes, and a candidate query at 499 does not gain more
authority than its declared bound. Feature bits and node tags remain distinct
inside the successor codecs. Contract V21 is independent because an
application can declare a pattern provider without yet deploying a query.

The topology file is not changed by WP-733: source assertions require the
constants and decoders to exist. WP-734, WP-736, and WP-737 add each node with
its implementation and compatibility fixture rather than registering a
fictional writer early.

## Bound and oracle inventory

`fixtures/riffql/bounded-filtered-result-v1/expected-v1.json` is the independent
materialized oracle. Its cases establish:

- every source completes before intersection, difference, sort, and page;
- duplicates are removed by canonical key equality;
- empty sources produce an empty complete set;
- one-over candidate or verification work refuses rather than truncates;
- policy removal occurs before positive-universe difference and sorting;
- binary and folded LIKE use exact retained-value verification;
- `%`, `_`, escaping, short patterns, and 8,000-byte source boundaries are
  accepted inside declared bounds; and
- 499, 500, 50,000, and 65,534 result rows are structurally admissible while
  65,535 is refused, independently of encoded-result bytes.

## Compatibility and implementation hazards

- Do not change `MAX_KEY_BYTES`; provider values never enter ordered storage
  keys.
- Do not increase exact-text V1's 256-byte or substring-term constants.
- Do not treat tokenized terms as wildcard or substring matches.
- Do not reuse `many ... take` for candidate completeness.
- Do not form a negative universe from physical rows before policy.
- Do not make the candidate's canonical internal order observable.
- Do not increase the 4 MiB result or transport ceilings while raising rows.
- Do not encode an arbitrary order list in generated input.
- Do not publish topology identities until their readers, writers, and frozen
  fixtures land together.

## Documentation impact

WP-733 changes no available public behavior. The public handbook remains
accurate and continues to describe candidates, long patterns, and pages above
499 as unavailable. WP-735 through WP-738 must update the affected handbook
pages and `docs/SUMMARY.md` when each behavior becomes executable.
