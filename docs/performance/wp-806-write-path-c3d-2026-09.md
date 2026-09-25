# WP-806: measured write-path attribution on C3D

Development evidence, 2026-09-22, under ADR-0239 and ADR-0257. This report
covers Ticketdesk writes and named-query reads. It does not qualify projection,
tokenized-text or row-policy workloads, and is not an N1/E2 release gate.

## Decision and measured outcome

The pre-change full-scale write comparison reaches **0.614 times safe-app
PostgreSQL throughput**, despite approximately 16 commands per RiffDB flush.
The earlier bank's structural write-advantage inference is withdrawn.

CPU samples identified repeated construction, formatting, allocation and hashing
of two immutable catalog declarations. ADR-0257 permits computing each digest
once, preserving its existing bytes and all stored-data checks. The fresh
three-revision comparison qualifies this repair: **4.5% more write throughput
at one client, 8.7% at 32 clients, and 6.4% more mixed throughput** than the
pre-change control. Read throughput is within noise. All four cells are within
the four-percent noise band of the banked revision: this recovers a regression,
not a demonstrated improvement over that bank.

The candidate full-scale write comparison reaches **0.659 times safe-app
PostgreSQL throughput**: 3,130 against 4,748 ops/s. Its p95 is 12.059 ms against
10.486 ms, a 1.150 ratio. Full-scale mixed throughput reaches **1.020 times
PostgreSQL**, with a **1.032 p95 ratio**: parity within this host's noise band,
not a demonstrated lead. The completed campaign qualifies retention of the
cache. WP-806 closes with measured attribution and these achievable ratios;
it does not close the remaining performance gap or release gates.

## Revisions and method

| Role | Revision | Compiler |
| --- | --- | --- |
| Banked jemalloc baseline | `97b90f6e18515b02d4e4d3b7a16c532cc0573508` | Rust 1.97 |
| Pre-change control | `74f05ad84dfdcb4f207612b95cb8168f617ad5fc` | Rust 1.98.1 |
| Catalog digest candidate | `b615e17df478fae17f27bb437eec11f68cbbb682` | Rust 1.98.1 |

Host: 8 vCPU, AMD EPYC 9B14, 32 GB memory, SHA-NI, ext4. Release builds use
locked dependencies, thin LTO and one codegen unit. The bank comparison includes
its historical toolchain; candidate versus control uses the same toolchain.
Each revision has its own checkout and binaries. One shared exclusion lock
serializes this campaign with other benchmark work. Databases are fresh for
each run. Source and binary hashes accompany the numeric evidence.

The initial locked control-runner build refused a stale nested CRC lockfile.
Commit `74f05ad84` synchronized it with the already accepted workspace graph;
the build then passed. No workload had started, and the refusal is retained as
a build failure, not a missing or retried performance cell.

Two campaigns are retained. The first compares bank/control over four smoke
cells and performs the control's full comparator and diagnostic runs. After
profiling identified the candidate, a second frozen plan rotates bank, control
and candidate in orders B/C/N, C/N/B and N/B/C, one order per repetition. It
uses write-only c1/c32, read-only c32 and interactive c32, three repetitions
each, with 20-second windows and five-second warmups. These 404-row smoke
measurements establish development deltas, not PostgreSQL parity.

The candidate retention rule was fixed before its runs: more than four-percent
median throughput improvement in at least one write cell, no greater-than-four-
percent loss in any cell, and every correctness check clean. All latency
observations are retained; latency was not substituted into the declared rule.
No run was retried or discarded for its performance. The two campaigns' control
numbers differ, so candidate claims use the fresh rotating campaign.

The full comparator uses 19,220 seeded rows across ten organizations, 32 clients,
90-second windows, 15-second warmups and three repetitions. Backend execution
is exclusive and counterbalanced: PG/R, R/PG, PG/R. PostgreSQL 18.4 reports
`fsync=on`, `full_page_writes=on`, `synchronous_commit=on` and
`wal_sync_method=fdatasync`. Full and smoke are never pooled. The harness
registers a cold board projection; these selected loads do not activate or query
it. The contract has no text index or row policy. Bodies use the full profile's
short generated labels, not the production profile's larger payloads.

## Fresh development comparison

Throughputs are medians with the complete three-repetition range, in ops/s.

| Load / clients | Bank | Control | Candidate | Candidate / control | Candidate / bank |
| --- | ---: | ---: | ---: | ---: | ---: |
| Write / 1 | 671 (670–678) | 639 (638–641) | 668 (668–675) | 1.045 | 0.996 |
| Write / 32 | 3,537 (3,489–3,540) | 3,302 (3,282–3,304) | 3,590 (3,580–3,624) | 1.087 | 1.015 |
| Read / 32 | 29,557 (29,545–29,572) | 28,932 (28,844–28,995) | 29,022 (28,991–29,063) | 1.003 | 0.982 |
| Interactive / 32 | 17,511 (17,478–17,582) | 16,329 (16,319–16,455) | 17,376 (17,354–17,474) | 1.064 | 0.992 |

The original bank/control campaign is also retained. Its median control write
rates were 591 and 3,320 ops/s at c1/c32. Comparing the candidate only with that
older, slower c1 result would overstate the gain.

## Full-scale comparator

Medians of three repetitions. Latencies are milliseconds. The paired PostgreSQL
rows belong to their respective campaigns; the control and candidate campaigns
were sequential, not interleaved.

| Campaign / load | Backend | ops/s | p50 | p95 | p99 | Create comment p50 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Control / write | RiffDB | 2,919 | 9.961 | 12.583 | 88.080 | 9.437 |
| Control / write | PostgreSQL | 4,756 | 6.554 | 10.486 | 14.680 | 6.291 |
| Candidate / write | RiffDB | 3,130 | 8.126 | 12.059 | 104.858 | 8.126 |
| Candidate / write | PostgreSQL | 4,748 | 6.554 | 10.486 | 14.680 | 6.291 |
| Control / interactive | RiffDB | 14,903 | 0.721 | 9.961 | 12.583 | 8.913 |
| Control / interactive | PostgreSQL | 15,731 | 1.180 | 8.126 | 11.010 | 6.816 |
| Candidate / interactive | RiffDB | 15,941 | 0.754 | 8.389 | 12.059 | 7.340 |
| Candidate / interactive | PostgreSQL | 15,622 | 1.180 | 8.126 | 11.534 | 6.816 |

All four full reports are eligible, stable, correctness-clean, complete in
resource attribution, and same-device comparable, with no missing required
fields. Candidate write rates are 3,116–3,138 ops/s; mixed rates are
15,901–16,027. The mixed point clears the numeric 0.90 throughput / 1.25 p95
criteria on C3D. PERF-018 requires N1/E2 evidence; this does not clear that gate.

The write tail remains a material limitation: candidate p99 is **104.858 ms,
7.14 times PostgreSQL**, and its median is higher than the control campaign's
88.080 ms. The predeclared throughput retention rule is not a tail-latency
acceptance gate. These sequential campaigns do not isolate a causal change in
tail latency, and the repair must not be described as a general latency win.

## Attribution: measured scopes and classification

Three control repetitions per client count sample a full-scale write workload.
Writer and service timers cover the entire measured daemon generation, including
warmup. Values below divide total stage time by that generation's committed
commands or service invocations, then take the median across repetitions. They
are amortized work/wait measurements, not request latency. Nested stages and
parallel preparation must not be added as an end-to-end decomposition.

| Component | Measured control evidence, c1 / c32 | Classification and implication |
| --- | --- | --- |
| Provenance-bearing record construction | Serial `stage_build_records`: 4.277 / 5.550 µs per command; detached preparation parent: 11.368 / 0.573 µs per command. Provenance-named self CPU: 0.074% / 0.139%. | Provenance and atomic record membership are guarantee-required. Owned values, cloning and current record assembly are representation costs. These measurements do not establish that all provenance costs only the named CPU percentage. |
| Audit | Service audit begin: 5.189 / 4.094 µs per invocation; writer audit prepare: 0.723 / 0.955 µs per command. Audit-named self CPU: 5.121% / 1.529%. | Audit transitions are guarantee-required. The existing object construction, encoding and allocations are representation-dependent; no audit transition was removed. |
| Journal encoding | Uninstrumented full c32: 3.90 µs per committed command. Canonical-codec self CPU in the diagnostic runs: 0.653% / 1.439%. | Canonical ordering, checked envelopes and recoverable journal records are guarantee-required. Their current serialization and buffer management are representation costs. Journal encoding is a distinct measured boundary, not all canonical encoding. |
| Journal append | Uninstrumented full c32 positional write: 2.02 µs per committed command. | Appending recoverable bytes is guarantee-required; byte volume and write mechanics depend on representation. |
| Durable sync | Uninstrumented full c32: 86.94 µs amortized per command, approximately 15.9 commands per physical flush. | Durability before acknowledgement is guarantee-required. Device latency and batching determine this measured cost; it cannot be removed by acknowledging early. |
| Allocation traffic | Allocator self CPU: 21.365% / 13.471%. Separate five-second traces record 913,594 / 1,303,649 malloc entries and 905,317 / 595,800 realloc entries. | Current owned representations cause real traffic, but allocation itself is not a semantic guarantee. Removing the closed-catalog portion is incidental-cost elimination; the rest was not proven removable here. |
| Closed catalog declarations | Catalog-named self CPU: 2.883% / 1.133%; formatting: 13.619% / 3.992%; hashing/checksum: 9.474% / 12.280%. | Reconstructing the same two input-free declarations on every call is incidental. Only this work is cached. These broader formatting/hash categories also contain unrelated work and are not claimed entirely removable. |

`stage_build_records` wraps `build_atomic_command_record_set` only in
`stage_checked_candidate`. Detached construction instead runs through
`prepare_detached_record_graph`, whose caller times the preparation pool as
`exec_prepare_detached`. The two timings name their real scopes; neither is an
isolated inclusive timer for the provenance constructor. Constructor entry
counts from the separate traces confirm that provenance construction remains
active. We do not infer its complete inclusive cost from a tiny self sample.

At c32 the control's uninstrumented pipeline additionally measures admission
10.35 µs, compatibility 1.28 µs, evaluation 37.89 µs, validation/encoding/staging
95.42 µs and publication 0.05 µs per committed command. These locate work around
the durable boundary; they are not extra amounts to add to the overlapping
diagnostic stages or journal timers. End-to-end latency also includes queueing,
batch waits and transport.

The candidate's uninstrumented full c32 journal measurements remain present:
encoding 3.85 µs, positional write 1.92 µs and durable sync 90.19 µs per command,
with 15.89 commands per physical flush. Median process CPU ticks per committed
command fall from 0.06491 to 0.05624 across the sequential full campaigns. These
resource scopes include warmup and are corroboration, not an isolated estimate
of time saved by the cache or a claim that sync became faster.

### CPU and allocator limitations

Hardware CPU events are unavailable on this VM. The capture uses software
`cpu-clock:u` at 997 Hz for 20 seconds, with DWARF stacks. It selects the exact
owned daemon only after two terminal setup censuses and the third startup-ready
census, waits 15 seconds past readiness, and verifies process identity before
and after capture. This excludes seed/recovery and warmup. Method corrections
were frozen before the first diagnostic capture.

Published categories are exclusive **self CPU** groups formed from resolved
symbols. They are lower bounds on semantic ownership, not inclusive call-tree
costs, wall time or hardware cycles. About 17–19% of control sampled time has
unresolved symbols. Perf also reports some thread-open warnings; each receipt
retains the count. Lost-sample counters are recorded separately. No claim of a
complete CPU decomposition depends on silently assigning these unknowns.

Allocator capture uses separate daemon runs and five-second uprobes after an
eight-second readiness margin. It counts jemalloc API entries and requested-size
arguments, plus provenance-constructor entries. APIs can nest: their counts
and byte arguments cannot be summed into successful allocations, resident bytes
or allocations per committed command. The control traces observe 1,123 / 2,314
provenance-constructor entries at c1/c32. Tracing is intrusive and its throughput
does not qualify a performance result. Raw CPU and allocator captures remain
private because they can contain machine identifiers; the publication contains
allowlisted numeric reductions and cryptographic hashes, not raw tool logs.

### Candidate diagnostic confirmation

Control values are medians of three captures; candidate values are one capture
per client count, as predeclared for confirmation. All values are percentages
of sampled user self CPU, not percentages of wall time saved.

| Symbol group | Control c1 | Candidate c1 | Control c32 | Candidate c32 |
| --- | ---: | ---: | ---: | ---: |
| Allocator | 21.365 | 7.818 | 13.471 | 10.136 |
| Formatting | 13.619 | 0.221 | 3.992 | 0.257 |
| Closed catalog declarations | 2.883 | 0.533 | 1.133 | 0.420 |
| Hash/checksum | 9.474 | 6.668 | 12.280 | 12.932 |
| Audit names | 5.121 | 10.101 | 1.529 | 1.740 |
| Canonical codec | 0.653 | 1.104 | 1.439 | 1.692 |
| Provenance names | 0.074 | 0.228 | 0.139 | 0.184 |
| Unresolved | 16.812 | 23.316 | 19.119 | 20.010 |

The expected catalog-formatting work largely disappears, while audit,
provenance, canonical encoding and hashing remain observable. Remaining catalog
symbols also include namespace operations needed by existing checks. Removing
one component changes every percentage's denominator; a larger audit/hash share
does not establish more audit/hash work per write. The eight CPU captures report
zero lost samples and four thread-open warnings in total. Unresolved samples
remain explicit, including the candidate's larger unresolved share.

The separate allocator captures also show a large drop in realloc entries,
while malloc entries do not uniformly fall. Complete five-second counts are:

| Capture | malloc entries | realloc entries | realloc size arguments, bytes | Provenance constructor entries |
| --- | ---: | ---: | ---: | ---: |
| Control c1 | 913,594 | 905,317 | 114,450,653 | 1,123 |
| Candidate c1 | 737,010 | 28,270 | 8,367,060 | 1,512 |
| Control c32 | 1,303,649 | 595,800 | 114,168,481 | 2,314 |
| Candidate c32 | 1,568,977 | 42,730 | 50,323,680 | 2,286 |

These are whole-process entry counts during intrusive sampling. Different
amounts of work complete in each window; the table must not be converted into
allocation savings per committed command. Provenance construction continues in
every capture. The computation-count test and unchanged fixture hashes establish
the exact bounded cache behavior independently of the profiler.

## Evidence reproduction

The [numeric evidence](wp-806-write-path-c3d-2026-09.json) contains all 76 planned
receipts, including the earlier comparison campaign, all latencies, eligibility
fields, stage totals, CPU categories, allocation counts, method amendments and
source/binary hashes. All workload outcomes are clean. The repository checker
recalculates medians and validates report completeness and the retention rule:

    python3 benchmarks/check-wp806-evidence.py

This verifies the published reduction, not the unavailable private raw captures.
An independent rerun must use the exact revisions, build profile, dataset,
client counts, phase ordering and durations above; it must not substitute a
smoke comparison for a full comparator result.

For each full comparator load, after building and selecting the exact revision's
daemon and runner binaries, the workload arguments are:

```bash
benchmarks/run-app-baseline --full --load write_only --load-clients 32 \
  --load-duration-secs 90 --load-warmup-secs 15 --reps 3 \
  --postgres-comparator safe-app --database-root ./fresh-benchmark-data \
  --output ./write-comparison.json
```

Use a distinct fresh database root and output for `--load interactive`. The
smoke comparison changes the profile to `--smoke`, uses 20/5-second windows,
`--reps 1 --skip-postgres`, and rotates the three revisions as specified above.

## Safety, validation and remaining limits

The production change caches two 32-byte declaration digests in separate
`OnceLock` cells. It adds no request-dependent cache, stored-data cache, public
knob, I/O, dependency or unsafe code. Provenance, audit, record hashing, durable
ordering, authorization and acknowledgement boundaries remain active.

The regression test first failed with 512 computations instead of one. With
the cache, eight barrier-synchronized cold callers repeatedly receive independent
hashes of the frozen V1/V2 fixtures, with exactly one computation per version.
The fixture/domain tests and scoped acceptance passed. Full `ci-all` passed on
the candidate, including 6,101 passing test reports. Known nonfatal tool notices
concern unrendered fixture/template manifests and the installed Maturin version;
operator wheel conformance passed. No test failure was ignored.

This work establishes a measured C3D development result. It does not erase the
historical N1/E2 production-scale gap or prove parity on those profiles. It does
not provide a theoretical upper bound on future optimization: such a number
cannot be derived by adding nested timers or assuming all allocation/hash work
is optional. POC-exit review must keep the unmeasured release profiles and
dormant feature coverage open.

## PR note

- **Package / tier:** WP-806 / guarantee.
- **Behavior added or changed:** two closed catalog declaration digests initialize
  once per process. The benchmark runner's nested CRC lock matches the accepted
  workspace dependency graph. This report supplies measured cost attribution
  and corrects the earlier unsupported full-scale write-advantage inference.
- **Checks run:** the computation-count test failed before the cache and passed
  after it; storage API acceptance and full candidate `ci-all` passed. Draft
  documentation acceptance passed after fixing a link from the rendered handbook
  to this repository evidence page. All 76 reports pass the evidence checker.
  Final `./scripts/acceptance --wp WP-806 --base HEAD` passed all nine checks,
  including scope, requirement coverage, ADR obligations and handbook validation.
- **Compatibility:** public signatures, canonical fixture bytes, persistent
  formats, verification calls and acknowledgement semantics are unchanged.
- **Documentation:** this report and numeric receipts; corrections in
  `c3d-programme-bank-2026-09.md` and a link from the existing handbook page
  `app-baseline-production-profile.md`. No new application interface is added.
- **Hazards and follow-ups:** full-scale write throughput still trails PostgreSQL;
  write p99 remains high. N1/E2 release qualification, larger payloads,
  projection/text/policy coverage and isolated inclusive provenance costs remain
  outside this evidence. The known limits are not waived by package completion.
