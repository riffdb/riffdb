# WP-449 through WP-473 closure audit

This audit closes the first mixed-load writer-attribution and optimization
campaign against current `main`. These packages are engineering investigations
and bounded implementation steps; they do not qualify the alpha release.

## Implementation and decision receipts

- `WP-449` shipped closed writer-stage evidence in `182fabec` and
  `docs/performance/wp-449-writer-evidence.md`.
- `WP-450` is satisfied by the current process graph: reads use the bounded
  blocking-port driver while commands enter the independently bounded sole
  coordinator. Command execution cannot consume read-port workers or create a
  second mutation owner; shutdown drains both owners explicitly.
- `WP-451` and `WP-452` shipped compiler-proved commutative child appends and
  authoritative write-amplification accounting in `d7b897ba` and `972aa7cb`.
- `WP-453` recorded a machine-readable proceed decision and shipped the bounded
  FIFO serial semantic protocol in `6cc7570d` and `097dac16` under accepted
  ADR-0095.
- `WP-454` completed as a failed release gate: the retained comparison proved
  parity had not been reached and kept alpha blocked. Later PERF-018 evidence
  packages supersede its benchmark corpus; its failure is not rewritten as a
  pass.
- `WP-455` through `WP-473` shipped in the contiguous optimization series
  `2fd9581d` through `b11909d0`. Their package-specific evidence is retained in
  `docs/performance/wp-455-*.md` through `wp-473-*.md` (with WP-465's immutable
  grant ownership covered by `5d1abadd` and current policy/type tests).

## Retained decisions

The campaign contains both positive and deliberately bounded results:

- columnar publication no longer materializes unobservable intermediate
  snapshots;
- the physical group ceiling is dynamically selected under the fixed 256
  safety ceiling;
- the first writer-feeding candidate was retained only at its measured knee,
  then WP-468 re-evaluated and safely raised the bounded batch after intervening
  CPU reductions;
- checked command graphs, permissions, events, wire reservations, and
  read-dependency comparisons reuse immutable or moved proof material without
  removing an authorization, canonicality, recovery, or acknowledgement safe
  point;
- WP-466 did not enable deferred multi-group durability epochs; WP-469 enabled
  only the narrower, immediately fenced single-subgroup protocol justified by
  the evidence; and
- command-audit access, commit revision dispatch/materialization, and audit-link
  sealing retain independent byte, semantic, and recovery validation.

## Current-head verification

The closure campaign's repository-wide all-feature tests, formatting, Clippy,
generated-artifact checks, requirement coverage, handbook checks, and
app-baseline self-test all passed. The current command, storage, concurrency,
lost-response, and process-recovery suites cover the retained semantics. The
focused columnar/projection suite also passed every unit, oracle, recovery,
holdback, and CP2a case.

## Release boundary

`WP-454`'s honest failed gate and all short same-host retain-or-revert
measurements remain historical engineering evidence. Current release
qualification is owned by `WP-623` and `WP-674`; endurance and final alpha
acceptance remain owned by `WP-578` and `WP-579`.
