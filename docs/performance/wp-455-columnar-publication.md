# WP-455 — Columnar catch-up publication amplification

Date: 2026-08-05

## Finding

`ApplyState::apply_available` held the engine mutex while applying a complete
bounded authoritative scan, but it cloned the complete accumulated columnar
delta into a new published snapshot after every commit. No query could acquire
the engine during that call, so every intermediate snapshot was unobservable.
With an expanding delta this performed quadratic derived-plane copying during
seed and catch-up.

The apply path now materializes the latest race-free snapshot once at the end
of a catch-up call. Before applying a commit whose authoritative post-image has
raced ahead, it first publishes the exact fully applied prefix accumulated in
the call. The held commit remains unpublished until its superseding commit is
processed. Direct single-commit unit entry retains its commit-boundary
publication behavior.

## Safety evidence

- A three-commit catch-up observes exactly one publication at the final
  frontier.
- A catch-up containing a safe commit followed by a forward-raced commit
  publishes the first commit, retains the second behind holdback, and advances
  to the complete frontier only after the superseder arrives.
- Existing randomized history, all-or-none multi-entity commit, checkpoint,
  compaction, crash/reopen, duplicate-apply, and projected-query equivalence
  tests remain green.
- The authoritative commit log, entity state, command path, durable formats,
  public protocol, authorization, and freshness outcomes are unchanged.

## Performance result

Before this change, a software CPU profile attributed about 13% of sampled
process CPU to the columnar worker, led by `CanonicalValue`, row-vector, and
whole-`BTreeMap` cloning. The full public-command seed after the change was
19,220 commands in 3.915 seconds (`target/app-baseline/wp455-columnar-publication.json`).
That is within the preceding 3.77–3.89 second run band rather than a critical
path improvement: columnar catch-up executes in parallel with the authoritative
writer.

Writer telemetry still attributes 2.448 seconds to 358 durable commits and
1.059 seconds to validation/encoding/staging. This optimization removes a
quadratic resource-growth hazard, but reaching a sub-3-second seed requires
fewer physical durability boundaries and/or less authoritative staging work.

## Documentation impact

None. This is an internal derived-state materialization optimization with no
user-visible behavior or interface change.
