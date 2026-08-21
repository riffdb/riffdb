# WP-656 exact-predicate semantic closure

WP-656 freezes the provider-independent half of ADR-0134. RiffQL language V6,
query IR V9, and query-module V9 now carry one canonical bounded predicate,
optional-presence, exact-count, ordinal-window, and independent total-order
program. The compiler emits the oldest sufficient identity: existing narrow
exact-result sources remain on their prior versions and retain their canonical
bytes.

The accepted model covers exact scalar comparisons, bounded canonical
`in`/`not_in` sets, missing/null state tests, binary UTF-8 prefix/suffix/
substring matching, bounded conjunction/disjunction, optional guards, and
mixed-direction order ending in the complete ascending entity key. A
standalone reference evaluator freezes two-valued missing/null behavior,
canonical set behavior, ordering, counts, and ordinal pages independently of
the future production provider.

The compiler rejects the whole family when any field, type, order term,
partition route, declared index, policy mode, exact-count requirement, or
static resource bound lacks proof. Generated Rust, Go, TypeScript, and Python
facades continue to expose only the named operation and typed parameters; none
contains a predicate AST or provider requirement.

This package intentionally does not activate execution. The indexed provider
and public runtime surfaces remain owned by WP-657 and WP-658. A V6 plan cannot
fall back to an entity scan, client-side shaping, an older provider, or partial
family execution.

## Verification receipt

- Focused syntax, IR, compiler, module, catalog, and four-language generation
  tests pass.
- The independent randomized semantic corpus and malformed/noncanonical codec
  tests pass.
- The required 60-second `riffql_parser` fuzz run completed 1,094,886
  executions without a crash.
- Version-topology, generated-artifact, requirement-coverage, and handbook
  checks pass.
- Workspace formatting and Clippy pass on Rust 1.97.0.

The nightly fuzz build reports only pre-existing standard-library deprecation
warnings in storage telemetry; the stable workspace Clippy gate remains clean.
