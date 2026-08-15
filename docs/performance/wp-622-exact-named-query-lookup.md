# WP-622 Exact Named-Query Lookup

WP-622 collapses a warm generated named-query resolution from separate contract
and module paths into one bounded immutable lookup. It changes no public query,
authorization, row-policy, freshness, or response semantics.

## Shape

The server publishes an immutable map keyed by the complete contract lineage,
contract version, contract hash, query-module hash, and checked query operation
name. Each entry owns the already-validated contract and module through their
existing shared `Arc` representations. A hit therefore performs one admission
precheck, one immutable-view read, one module lookup, and cheap shared clones.

The view retains at most 4,096 exact modules, matching the existing hot-module
bound. Publishing a cold or replacement module builds a bounded successor view
and swaps it atomically. Contention, a miss, or an identity mismatch uses the
authoritative contract/module path. A stopped, draining, cancelled, or expired
request is never answered from the process-local view.

The view contains no principal, capability, policy result, parameter, row, or
response. Authentication, begin authorization, capability-revision checks,
row-policy evaluation, execution fuel, read-after-commit fencing, post-execution
authorization, pre-release authorization, and redaction still execute for every
request.

## Workstation diagnostic

A correctness-clean 32-client interactive smoke run used the public gRPC path
for five measured seconds after one second of warmup. Named-query plan lookup
recorded 154,650 observations and 892,238 microseconds in total, or about 5.77
microseconds per request. This is diagnostic evidence rather than the WP-623
qualification; the cloud 128-client gate remains required before WP-622 closes.

```bash
./benchmarks/run-app-baseline --full \
  --load interactive \
  --load-clients 32 \
  --load-duration-secs 5 \
  --load-warmup-secs 1 \
  --reps 1 \
  --skip-postgres \
  --database-root "$HOME/tmp/riffdb-wp622-smoke-db" \
  --output "$HOME/tmp/riffdb-wp622-smoke.json"
```

The smoke produced 30,607 operations per second with zero public errors,
conflicts, or idempotency mismatches. The final cloud result is recorded here
only after the same revision has run on the inventoried cloud profile.
