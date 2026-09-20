# Sub-variance wins register

A change can be real and still be too small to measure. The C3D bench host's
run-to-run variance is 2 to 4 percent, and ADR-0239 decision 3 refuses a
single-run delta smaller than that as a result. Taken literally on each change
in isolation, that rule would refuse every improvement under about 3 percent
forever — which in a database, at scale, is most of them.

This register is how those changes get taken anyway without weakening the rule.
An entry is **implemented and merged** on its own evidence, and its system-level
effect is **measured in aggregate** with the others rather than claimed alone.

## What qualifies

1. **Measurable where it is real.** The win is demonstrated at the level it
   occurs — a primitive benchmark, an algorithmic complexity change, a removed
   allocation — with numbers, not an argument.
2. **Behaviour-preserving, and proven so.** Byte-identical output, or identical
   observable state, with a test that fails if that stops being true. The test
   is the deliverable, not the speedup.
3. **No system-level claim.** Nothing here is banked, quoted in a release note,
   or used to justify relaxing a reviewed boundary, until the batch is measured
   together on the bench host against the previous banked revision.

## Open entries

| change | measured effect | estimated share of writer busy | evidence |
|---|---|---|---|
| Hardware CRC-32C for envelope checksums | 5.54 → 9.52 GB/s at the 6,393-byte frame size, 1.72x, 482 ns saved per call | ~0.34% (encode and decode) | `crc_matches_the_software_table_across_sizes_and_alignments` |
| `long_pattern` incremental postings | O(N^2) → O(N); 250/500/1000 inserts at 5.25/11.09/23.61 ms | dormant; no contract declares the provider | `incremental_maintenance_equals_a_full_rebuild_over_the_same_rows` |

## Deferred, not yet taken

| candidate | why deferred |
|---|---|
| `chunks_exact_to_as_chunks` (72 sites, Rust 1.98 lint) | Deferred by ADR-0241 so the toolchain bump measured a compiler change alone. Unblocked now; unmeasured. |
| Remaining dormant-path repairs (per-query `CorpusStatistics`, materialize-before-offset, per-row policy scans) | Recorded in `deferred-dormant-path-optimizations.md`; all on paths no contract declares today. |

## When to measure the batch

When the estimated aggregate share reaches roughly 3 percent — the point at
which one C3D run against the previous banked revision can distinguish it from
noise. Measure once, bank once, under ADR-0239's discipline. If the aggregate
does not show up, that is the result, and it is worth knowing: it would mean the
per-primitive numbers are not reaching the system, which is the same failure the
2026-09 capture attribution nearly shipped a durable-format change on.

## What this register is not

It is not permission to skip measurement. Every entry carries a real number at
the level it was taken. It is a refusal to let "too small to measure alone"
become "never worth doing", while still refusing to claim what has not been seen.
