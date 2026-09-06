# WP-711 columnar V2 production-activation receipt

Status: **integrated production-path receipt banked and independently
validated; WP-711 is complete on the current-main-integrated tree**.

WP-711 uses the frozen WP-710 mechanics gate as a mandatory preflight, then
measures one production-shaped V1/V2 activation corpus. The generator writes no
receipt unless the unchanged ADR-0160 mechanics thresholds and every activation
control pass. It never substitutes a model for a missing metric.

## Frozen method

The activation corpus contains 16,384 primary-key-ordered `Ticket` rows at one
matched authoritative frontier, divided between two compiled organization
partitions of exactly 8,192 rows. It uses four deterministic low-cardinality
titles and statuses, monotone primary keys and priorities, five unrecorded
warmups, and 31 release-mode samples. Each production request carries exactly
one compiled organization scope and performs one ordinary bounded count. The
private evidence referee alone sums the two 8,192-row results; neither runtime
nor a public query crosses the frozen aggregate arithmetic limit. The receipt
retains both per-partition p50/count fields and combined two-query timings/count.
The V1 control is built by the server columnar worker from the authoritative
redb snapshot and published through the durable common control. The V2 arm uses
that same worker to allocate, stream, completely validate, and publish the
immutable generation through the transaction-current CAS and capture gate. The
test registers the real acknowledgement observer before publication, rereads
the durable selection, and proves repeated request captures reuse the exact
selected `Arc`. Queries use those request-captured immutable views. Recovery
measurements use the server's controlled-generation reopen seam, and compaction
uses a fresh durable candidate plus the same worker/CAS/gate/acknowledgement
path. The older `riffdb-columnar` diagnostic remains explicitly non-terminal
and emits a different marker.

CPU fields are **single-thread elapsed ns** measured with `Instant`; they are not
hardware-counter samples. Allocation fields are the accepted **WP-710
modeled-owned-allocation metric (not actual allocator calls)**. The frozen model
counts six retained owned values per row and adds the accepted 17 bounded
retained-view objects per V2 segment. It does not claim to count allocator
invocations.

Physical byte fields sum the bounded files beneath the exact test generation.
Projection lag is authoritative head minus selected frontier. The no-projection
control asserts that its directory never exists and therefore has zero bytes,
modeled owned allocations, and population passes. The no-V2 control is the
ordinary V1 checkpoint and query path over the same rows and frontier.

The generated JSON schema is
`riffdb.wp711.columnar-v2-activation-receipt.v1`. It pins the clean 40-character
implementation revision, Rust 1.97 release profile, corpus identities, sample
count, exact commands, raw marker lines, host CPU/load metadata, measurement
terminology, all metrics, and a passing verdict. The validator enforces every
exact nested key set, verifies that the pinned revision is an ancestor whose
production and evidence sources are unchanged in the current checkout, and
refuses unknown, missing, duplicated, malformed, empty, or out-of-bound marker
fields. Its self-test proves nested-schema and source-identity refusal, each
frozen WP-710 threshold, nonzero activation lag/control fields, a mismatched
result count, and any missing production worker/gate/acknowledgement evidence.

## Integrated production-path receipt

This receipt qualifies the production activation and non-regression evidence
only. WP-711 closure additionally relies on the accepted ADR-0200
stale-incarnation recovery proofs named in the package brief.

The generated receipt is
`docs/performance/wp-711-columnar-v2-activation.json`. Its schema is
`riffdb.wp711.columnar-v2-activation-receipt.v1`, its SHA-256 is
`864ac99e7ee2c952fd43ee705a1767ca7dc8ef5a87dfb3de5584d23248add65c`,
and it is pinned to the clean integrated implementation and
evidence-infrastructure revision
`5653fd908c9e4b288ac76dc9a3e281cb0a389f6f`. The receipt was generated with
Rust 1.97.0 in release mode on an AMD Ryzen 9 7950X with 32 logical CPUs. The
recorded load averages before and after the run were
`21.95 29.69 35.10` and `24.33 28.50 34.25`, respectively.

The unchanged WP-710 preflight passed every ADR-0160 threshold:

| Gate | V1 | V2 | Verdict |
|---|---:|---:|---|
| High-cardinality bytes | 1,851,404 | 1,609,476 (8,693 bps) | Pass; maximum 11,000 bps |
| Low-cardinality bytes | 1,421,324 | 785,287 (5,525 bps) | Pass; maximum 7,500 bps |
| Full-decode p50 | 5,773,468 ns | 4,553,194 ns (7,886 bps) | Pass; maximum 10,500 bps |
| Clustered segment rejection | — | 99/100; zero false negatives | Pass; minimum 90 percent and zero false negatives |

The production-activation corpus also passed every fail-closed control. Each of
the two queries returned its exact 8,192-row partition count, and the private
referee obtained the combined 16,384 rows at matched frontier 16,384. Projection
lag, no-projection bytes, no-projection modeled allocations, and no-projection
population passes were all zero. Six worker passes produced three durable
publication acknowledgements, selected V2 generation 2, compaction generation
3, and pointer-equal reuse of the exact selected immutable `Arc`.

| Observation | V1/no-V2 control | Selected V2 |
|---|---:|---:|
| Physical bytes | 1,372,395 | 841,935 |
| Modeled owned allocations | 98,304 | 98,338 |
| Combined bounded-query p50 | 5,536,187 ns | 6,043,679 ns |
| Recovery p50 | 11,999,708 ns | 15,999,970 ns |
| Rebuild elapsed | — | 302,368,036 ns |
| Compaction elapsed | — | 292,123,064 ns |

V2 uses 38.65 percent fewer physical bytes in this production-shaped corpus;
its combined hot-query p50 is 9.17 percent above V1. Cold V2 recovery p50 is
33.34 percent above V1 because reopening pays complete selected-generation
validation before an immutable view becomes available. Those latency deltas are
review hazards, not hidden failures. These single-host observations are package
activation evidence, not public latency, storage-ratio, or allocation promises.
The allocation figures remain the WP-710 modeled-owned-allocation metric and are
not actual allocator calls. The retired pre-integration receipt remains
available in Git history but is not evidence for this integrated source tree.

## Exact commands

From a clean worktree at the final implementation and evidence-infrastructure
revision:

```bash
./scripts/wp711-columnar-v2-receipt --self-test
./scripts/wp711-columnar-v2-receipt
./scripts/wp711-columnar-v2-receipt \
  --validate docs/performance/wp-711-columnar-v2-activation.json
```

The generator first runs this unchanged frozen gate, serially:

```bash
cargo +1.97.0 test --release -p riffdb-columnar \
  wp710_fixed_corpus_mechanics_receipt -- --ignored --nocapture --test-threads=1
```

Only after it passes does the generator run:

```bash
cargo +1.97.0 test --release -p riffdb-server --lib \
  columnar_adapter::tests::wp711_production_v2_activation_receipt \
  -- --ignored --exact --nocapture \
  --test-threads=1
```

Generation refuses a dirty worktree and refuses to replace an existing receipt.
The script accepts only a rustup cargo proxy that proves the exact 1.97.0
selector. It tests `cargo` from `PATH` first, then the conventional
`$CARGO_HOME/bin/cargo` proxy, and fails rather than using an unpinned binary.
Its self-test places a rejecting cargo earlier on a synthetic `PATH` and proves
that the validated 1.97.0 proxy is selected. It also proves unique marker capture
from Cargo's stderr channel as well as stdout, including libtest's exact
`test ... MARKER` line prefix, because Rust test harnesses may emit nocapture
output through either inherited stream. The JSON was created only after both
child processes and independent marker validation succeeded, and the checked-in
receipt independently passes the validator command above.
