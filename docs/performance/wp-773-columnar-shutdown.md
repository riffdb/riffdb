# WP-773 bounded columnar shutdown

WP-773 activates worker-only columnar abandonment at an existing 64-record
authoritative page boundary. It does not change ordinary catch-up publication,
notification, checkpoint, freshness, or retention semantics. A graceful stop
may discard only unpublished in-memory derived work; restart resumes from the
last durable projection frontier and replays authoritative history.

## Semantic and crash evidence

The deterministic tests cover three distinct claims:

- `ordinary_columnar_apply_publication_and_checkpoint_are_unchanged` compares
  the worker no-stop path with the ordinary path through exact end, including
  progress, visible state, durable checkpoint, and query results.
- `shutdown_abandons_only_unpublished_columnar_work_at_a_page_boundary` uses an
  explicit barrier after a complete page. The stop publishes, notifies, and
  checkpoints nothing and advances no durable frontier; an earlier safe-prefix
  publication remains visible.
- `abandoned_columnar_shutdown_replays_from_the_durable_frontier` exits a real
  child process after abandonment, reopens the derived plane at its prior
  durable frontier, and converges to the uninterrupted rows and frontier.

The production worker owns the monotonic stop token. The internal observation
is closed to `between_passes`, `abandoned_unpublished`, or `failed`; it carries
no path, identity, frontier, population count, key, value, hash, credential, or
caller-supplied control. No application, SDK, transport, CLI, or operator
surface can select this behavior or its page size.

## Paired production-profile result

`benchmarks/run-wp773-paired` ran the exact pre-change server at `a6064df4`
against candidate `341fb5cd` on the full 19,220-command profile. Each side used
three repetitions in counterbalanced order, 32 warmups and 500 measured calls
for both the synchronous and asynchronous transport shapes. The host preflight
and postflight were valid with zero observed steal time.

| Qualifying metric | Control median | Candidate median | Candidate/control | Gate |
| --- | ---: | ---: | ---: | --- |
| Columnar shutdown | 101,938 us | 24,331 us | 0.239x | pass, <= 1.05x |
| Complete lifecycle shutdown | 859,644 us | 709,310 us | 0.825x | pass, <= 1.05x |
| Synchronous mean | 284,428 ns | 285,860 ns | 1.005x | pass, <= 1.05x |
| Asynchronous mean | 239,107 ns | 234,725 ns | 0.982x | pass, <= 1.05x |
| Synchronous throughput | 3,507 ops/s | 3,490 ops/s | 0.995x | pass, >= 1/1.05x |
| Asynchronous throughput | 4,177 ops/s | 4,255 ops/s | 1.019x | pass, >= 1/1.05x |
| Process CPU | 13.54 s | 13.78 s | 1.018x | pass, <= 1.05x |
| Process maximum RSS | 345,916 KiB | 344,532 KiB | 0.996x | pass, <= 1.05x |
| Server PSS | 111,172 KiB | 113,219 KiB | 1.018x | pass, <= 1.05x |
| Process wall | 6.99 s | 6.64 s | 0.950x | pass, <= 1.05x |

Histogram p95 buckets and the individual seed-generation columnar stage remain
informational because their bucket spacing cannot resolve a five-percent gate.
The exact arithmetic-mean request metrics, throughput, aggregate lifecycle,
CPU, PSS/RSS, and process wall measurements are qualifying.

## Evidence custody

The fixed summary remains at
`/home/kevin/tmp/wp773-evidence-current/summary-v1.json`, SHA-256
`cea0ab9057198a16db5310e85e376273762023a1a2486972606abcedf87adfd2`.
Its bounded receipt list carries the six raw report and resource-file hashes.
The exact binaries and runner were:

- control `riffdbd`: `2ca8bdc1293179caef5b92523c8f04cfa3a93df8ca2e87c59f0bc36451dc7605`;
- candidate `riffdbd`: `a1bf720f3576d31f39e254b7faf77eda4b169d7b078e65060cea907b72cd1978`;
- diagnostic: `a5b147383f1d86405d52a96b4f36c067d3fc62ec39265846d4dfe87ca2a91db5`;
- runner: `9d7e7828da8c516737b681462ae933c6db57b0bc6c5e7465b975c79b46c9fe3f`.

The qualifying host was a 32-logical-CPU AMD Ryzen 9 7950X workstation running
Gentoo. This receipt establishes the package's paired local production-profile
gate; it is not presented as a cross-host or PostgreSQL comparison.
