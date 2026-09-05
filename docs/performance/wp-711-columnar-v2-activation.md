# WP-711 columnar V2 production-activation receipt

Status: **evidence harness implemented; no receipt claimed by this revision**.

WP-711 uses the frozen WP-710 mechanics gate as a mandatory preflight, then
measures one production-shaped V1/V2 activation corpus. The generator writes no
receipt unless the unchanged ADR-0160 mechanics thresholds and every activation
control pass. It never substitutes a model for a missing metric.

## Frozen method

The activation corpus contains 16,384 primary-key-ordered `Ticket` rows at one
matched authoritative frontier. It uses four deterministic low-cardinality
titles and statuses, monotone primary keys and priorities, five unrecorded
warmups, and 31 release-mode samples. The V1 control is built through the
ordinary production projection engine and checkpoint path. The V2 arm prepares
and completely validates an immutable generation, compares the registered query
corpus with V1, installs the validated view in the production engine, reopens it
from durable bytes, and prepares a disjoint compaction generation.

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
terminology, all metrics, and a passing verdict. The validator refuses unknown,
missing, duplicated, malformed, empty, or out-of-bound marker fields. Its
self-test proves refusal at each frozen WP-710 threshold and for nonzero
activation lag/control fields or a mismatched result count.

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
cargo +1.97.0 test --release -p riffdb-columnar --test v2_generation \
  wp711_production_v2_activation_receipt -- --ignored --exact --nocapture \
  --test-threads=1
```

Generation refuses a dirty worktree and refuses to replace an existing receipt.
The JSON is created only after both child processes and independent marker
validation succeed. A later evidence-only commit may add the generated receipt;
this infrastructure revision intentionally contains none.
