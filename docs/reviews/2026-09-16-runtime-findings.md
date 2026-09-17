# September 16 runtime review disposition

Reviewed `/tmp/riffdb_code_review_findings.md` against base commit `6416894e`.
The report contains 20 allegations, several with code snippets or line numbers
that do not match this checkout. Thirteen findings are repaired; seven claims
are rejected or already addressed. The eight resource-cost repairs are covered
by accepted `adr/0234-bounded-repairs-for-reviewed-runtime-resource-costs.md`
and WP-785. Exact-text acceptance was recorded separately in commit `49fcd4bf`;
package registration preceded implementation in `13f90551`.

## Individual findings

| # | Disposition | Evidence and action |
|---|---|---|
| 1 | Fixed | `predicate_values_match` reached a `scalar_order` without Boolean, bytes, decimal or money cases. Share their typed comparisons with aggregate ordering. Decimal types and money currency/types must match; unsupported pairs remain refused. Tests cover predicate execution and signed values. This does not widen the compiler's physical access matrix. |
| 2 | Fixed | `predicates_match` used null-first sort order to decide range membership. Explicitly exclude null cells from ranges; equality and sort ordering are unchanged. Regression exercises the real nearest-query path. |
| 3 | Intentional public limit | The separate ad-hoc projected-query surface admits integer sums only: `riffdb-service/src/projected_query.rs` maps the fold refusal to typed `type_mismatch`. `docs/known-limitations.md` documents typed rejection for decimal/money. `AggregateValue::Sum(i128)` and `ExactMean` do not carry decimal scale. Adding an unscaled coefficient branch would silently misrepresent values. Named operational aggregates use their separate compiler-scale-aware path. |
| 4 | Fixed; severity corrected | `dispatch_once` advanced past the entire page before processing it. Advance after each successfully processed/deferred item and install exact-end/wrap state only after the complete page. Failures preserve the interrupted item and suffix. The old worker eventually wrapped at `ExactEnd`; permanent loss was not demonstrated. Tests cover interruption and completion `StateChanged`, with the latter still returned as an error. |
| 5 | Already addressed | `JournalSubmission::drop` completes its waiter with `JournalIoError::Stopped`. A deferred submission dropped on worker exit therefore wakes its waiter. Completion is idempotent, so successful or explicit error results are preserved. The report omitted this destructor. |
| 6 | Claimed serialization not demonstrated | `CommandSegmentPreparationPool` releases the receiver mutex in an explicit inner block before preparing a task. Dequeue is serialized, execution is concurrent. An idle receiver holding the mutex does not stop a worker already processing a task. No evidence justifies a new channel dependency or an unbounded queue. |
| 7 | Claimed serialization not demonstrated | `CommandEvaluationPool` has the same explicit receive scope, ending before its task match/evaluation. The report's compressed snippet omits that scope. No execution serialization defect is present. |
| 8 | Fixed | Published indexes retain complete exact locator, manifest and event metadata plus a payload cache capped at 64 entries and a conservative 64 MiB decoded-ownership charge. The cache fills lazily from canonical storage decodes, so caller spare capacity cannot escape the bound and publication does not re-encode/decode payloads. Cold and cached lookup use the caller’s captured authority and reject missing/corrupt/mismatched bytes. Rebuild streams segments; follower replacement removes prior metadata after eviction. Exact metadata remains proportional to retained history. |
| 9 | Rejected: unreachable premise | `prepare_projection_apply` creates a row update for every grouped delta, including a zero delta. Every successful push installs those keys in both observations and overlay; a refused push installs neither. Thus a previously observed key without an overlay cannot arise. The proposed map fallback would mask a broken invariant rather than repair an observed desynchronization. |
| 10 | Fixed | Scratch lanes share at most 16 buffered writers with 64 KiB buffers. Eviction and build completion explicitly flush and propagate failures. A deterministic writer probe proves one open for 100 same-lane appends, descriptor/buffer bounds, identical framing and failed eviction/final flush refusal. |
| 11 | Fixed | Nearest execution now calls `merged_org_bounded` with the request's existing scan ceiling before admitting candidates. Oversized partitions produce the same typed scan-budget refusal without cloning the whole partition. Tests cover refusal before any admission and success exactly at the bound. Existing org selection and version supersession remain intact. |
| 12 | Fixed | Uniform merge refills buffer at most 64 rows, charge every scanned/materialized row, and preserve epochs and policy. Speculation consumes only the runtime request’s unused compiler row allowance. At maximum limit, lazy heads remain until one stream owns the guaranteed remainder. Boundary tests cover limits 1, 2, 63, 64, 127 and 128 against ordered rows and a 129-point ceiling. |
| 13 | Fixed | One snapshot-scoped statistics object computes unique-term document frequencies and field lengths for identity and all candidate scores. Repeated terms retain their contribution. Tests pin existing exact scores, doubled-term scores and identical statistics identity, with posting-enumeration counts independent of candidate count. |
| 14 | Fixed; example corrected | Greedy earliest-position selection can omit valid proximity paths. Retain every reachable position with a monotonic predecessor cursor. Regression uses positions `a@0`, `b@1,4`, `c@8`, distance 4; exhaustive small-posting comparison covers phrase adjacency, repeated terms, gaps and empty lanes. The report's own `1 -> 10` example with distance 5 is not a valid path. |
| 15 | Rejected as a production correctness bug | `merge_work_ceiling` charges real `Vec::insert` shifts, explicitly explained in the code. This is not a linear merge whose charge can safely become a sum. The private batch module is inert and sealed by `check-columnar-batch-architecture`; lowering the charge would under-account actual work. |
| 16 | Fixed | Group-key encoding reuses scratch and borrows source cells. Retained key cells and bytes are cloned only for a new group. Existing state/cardinality fuel remains. Tests compare canonical framing and prove the scratch pointer/capacity is reused across repeated keys. |
| 17 | Fixed | Vector restore removes redundant re-encoding after strict canonical decode, exact length/tag/dimension and schema checks. Existing checksummed-corruption tests still reject non-finite components, negative zero, wrong tags/dimensions and truncated/trailing bytes. |
| 18 | Inert design cost | Sorted-vector insertion really shifts entries, and the work budget counts those shifts. This private, explicitly inert batch owner is structurally sealed. Its map replacement is not a live-query repair and is excluded from the proposal. |
| 19 | Fixed | Mixed-direction streams sort each new page with a fallible in-place sort and merge into the accumulated run. Physical-prefix completion and duplicate checks remain. Existing cursor tests and an independent mixed-order oracle cover ties and ordering. |
| 20 | Fixed | Mixed-order runs now use one fallible k-way heap and one output vector. Tests compare five runs (including an empty run) against an independent sort at seven output boundaries, preserve discarded-row detection and propagate missing-key comparison errors. |

## Review boundary

ADR-0234 admits only the enumerated live repairs under the existing performance
freeze. No critical dependency, durable encoding, storage key, application
option or authorization behavior changes. Inert batch/group mechanics and their
conservative shift charges remain sealed. The payload charge deliberately favors
eviction over tight packing; large segments remain readable without retention.

## Development performance sentinel

The unchanged short-window sentinel remains red. The runtime baseline at
`13f90551` (before implementation) failed two cells after automatic retries;
the repaired working tree failed all four. Both ran sequentially on the same
AMD Ryzen 9 7950X host, with one-second warmup and five-second measurement
windows. All cells reported zero non-success outcomes and clean correctness.
These are development signals, not release qualification or proof of throughput
neutrality. In particular, the baseline failure does not excuse the additional
misses or lower measured rates in this change.

The unedited [baseline receipt](2026-09-16-baseline-sentinel.json) and
[repaired receipt](2026-09-16-repaired-sentinel.json) retain every threshold,
observation and failure. Final observations, including the script's automatic
retry of failing cells:

| Cell | Baseline ops/s | Repaired ops/s | Baseline seed ms | Repaired seed ms | Baseline / repaired write p50 ms |
|---|---:|---:|---:|---:|---:|
| interactive-c1 | 1977 | 1933 | 2937.5 | 3576.9 | 1.966 / 1.966 |
| interactive-c32 | 26248 | 25309 | 2984.1 | 3591.0 | 6.554 / 6.554 |
| interactive-c128 | 29298 | 24952 | 2875.7 | 3677.3 | 25.166 / 29.360 |
| write-only-c32 | 3650 | 3579 | 3428.5 | 3773.8 | 7.602 / 8.913 |

The cache retains at most 64 entries and a conservative 64 MiB ownership charge.
The charge includes canonical tree layouts and variable buffers, not only wire
bytes. Cache population is lazy; publication performs no extra payload
encoding/decoding. Complete exact-index negatives open no payload read view.
Positive lookups validate the caller's captured authority, including cache hits;
cold segments require decoding. The tests prove these mechanics and bounds,
including a cacheable 64-command group and eviction during a paused read. They
do not establish a throughput win for fresh command traffic. The remaining
performance gap needs follow-up; no threshold, integrity check or acceptance
exclusion was relaxed to hide it.

Reproduce after other test/build workloads finish:

```bash
RIFFDB_TMP_ROOT="$HOME/tmp" ./scripts/app-baseline-performance-sentinel --output target/app-baseline/review-sentinel.json
```

The script also needs a fresh
`target/app-baseline/performance-sentinel` directory; preserve prior receipts
before moving it aside. `--self-test` passes independently of measured results.

## PR note and closure

- **Package:** WP-785 (plus findings 1, 2, 4, 11 and 14 from the initial review).
- **Tier:** guarantee, under separately accepted ADR-0234.
- **Behavior added or changed:** typed scalar range comparisons, null-safe
  ranges, outbox cursor preservation, bounded nearest materialization, complete
  proximity matching, bounded command payload/writer retention and the eight
  approved query/resource-cost repairs described above.
- **Checks run:** targeted semantic regressions, exhaustive positional oracle,
  independent partition oracle, injected writer failures, cache eviction and
  corruption tests, follower split/retirement comparison with streamed rebuild,
  old redb pin checks, deterministic cache eviction/read scheduling, decoded
  ownership accounting, exact negative lookup view counts, existing strict
  vector fixtures and storage crash tests. Final acceptance is recorded below;
  the failed sentinel and baseline comparison are retained above.
- **Compatibility:** no schema, durable encoding, wire field, cursor format,
  dependency or application option changes. Formerly omitted valid matches can
  appear; null range matches disappear. Existing refusals and fuel remain.
- **Updated handbook pages:** operational queries, vector search, tokenized
  text, domain events and startup integrity, all reachable from `docs/SUMMARY.md`.
- **Hazards:** cold lookups add I/O/decode; complete exact metadata still scales
  with retained identities. The sentinel is red, with several measured rates
  below baseline. Performance acceptance remains unresolved; mechanism counts
  are not throughput claims.

Completed on September 16, 2026 with status
`complete_with_failed_performance_gate`. The complete `./scripts/ci-all` run
passed: workspace formatting, clippy, tests, real-binary archive recovery,
xtask, rustdoc, dependency checks, adapter conformance, governance, generated
artifacts, Helm checks, public-source authoring and release installation smoke.
The workspace test phase reported 5,651 passed and zero failed; existing
explicitly ignored tests remain, with the two real-binary archive cases run
separately by CI. Final scoped acceptance
(`./scripts/acceptance --wp WP-785 --range HEAD..HEAD --no-tests`) passed all
12 checks, covering the closure and handbook; the complete CI run supplies
the final source tree's test evidence.

Dependency checking passed with duplicate-version warnings from the unchanged
lockfile. The adapter build emitted an environment warning because installed
maturin 1.15.0 differs from the project's 1.14.1 build requirement; wheel builds
and conformance passed. The sentinel self-test passed, but its measured gate
failed as detailed above. Performance acceptance remains unresolved.
