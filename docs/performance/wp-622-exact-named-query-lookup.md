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
recorded 131,029 observations and 229,377 microseconds in total, or about 1.75
microseconds per request. This is diagnostic evidence rather than the WP-623
qualification.

```bash
./benchmarks/run-app-baseline --full \
  --load interactive \
  --load-clients 32 \
  --load-duration-secs 5 \
  --load-warmup-secs 1 \
  --reps 1 \
  --skip-postgres \
  --database-root "$HOME/tmp/riffdb-wp622-sync-local-db" \
  --output "$HOME/tmp/riffdb-wp622-sync-local.json"
```

The smoke produced 25,371 operations per second with zero public errors,
conflicts, or idempotency mismatches.

## Cloud gate

The inventoried four-core GCP profile ran the registered 128-client cell after
the server and benchmark were updated to use the same exact contract hash that
generated clients embed. The prior three c128 repetitions averaged 99.52
microseconds per named-query plan lookup (97.72, 103.08, and 97.77). Revision
`234efa60` recorded 35,994 lookups and 519,433 microseconds in total, or 14.43
microseconds per lookup: a 6.90-times reduction. The required four-times gate
passes.

The five-second diagnostic completed 35,240 logical operations with no public
errors, conflicts, overloads, unavailable results, or idempotency mismatches.
Its absolute throughput remains writer-limited on this cloud profile; WP-621
targets that independent command-segment cost before WP-623 performs the full
90-second comparator qualification.

```bash
./benchmarks/run-app-baseline --full \
  --load interactive \
  --load-clients 128 \
  --load-duration-secs 5 \
  --load-warmup-secs 1 \
  --reps 1 \
  --skip-postgres \
  --database-root "$HOME/tmp/riffdb-wp622-cloud-c128-234efa60-db" \
  --output "$HOME/tmp/riffdb-wp622-cloud-c128-234efa60.json"
```

This short run closes WP-622's stage-reduction gate, not the final performance
qualification. WP-623 still requires the frozen evidence windows, both hardware
profiles, and both PostgreSQL comparators.
