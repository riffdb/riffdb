# WP-475 Retained Canonical Permission Lookup

WP-475 optimizes the inside of a complete current-policy authorization. It does
not remove, cache, combine, or revision-shortcut an authorization safe point.

Capability permission sets are already bounded, canonically sorted, duplicate
free, immutable, and shared by capability-grant clones. Their exact canonical
keys are now retained with that checked collection. Exact membership computes
the requested key once and binary-searches the retained keys instead of
rebuilding a candidate key for every comparison.

Current authorization still reloads the live capability record, samples fresh
authorization time, checks authenticated and current identity, validates expiry
and activity, checks the exact permission and approval requirement, derives
tenant, partition, field, row, audit, and output obligations, and produces a new
authorization proof. Denial and unavailable results remain fail closed.

Public constructors, permission slices, semantic bytes, durable encodings,
decoded grants, equality, and redaction do not change. Retained keys are an
in-memory acceleration derived only from the same canonical permission values.

## Same-host evidence

The full public 19,220-command seed was run five times for the retained-key
candidate and for the immediately adjacent exact prior lookup. The candidate
averaged 2.133 seconds versus 2.228 seconds for the control, about 4.3% faster.
Representative `create_comment` p50 was 1.705 ms versus 1.971 ms, about 13.5%
lower. A 999 Hz CPU profile reduced the post-evaluation authorizer itself to
about 0.11% of total sampled CPU while retaining the full policy safe point.

Artifacts:

- `target/app-baseline/wp475-retained-permission-keys.json`
- `target/app-baseline/wp475-permission-paired-control.json`
- `/home/kevin/tmp/riffdb-wp475-permission.perf.data`
