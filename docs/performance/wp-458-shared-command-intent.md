# WP-458 Shared Command Intent

WP-458 removes repeated deep copies of two immutable, already checked command
artifacts. `PreEvaluationCommitContext` and `CommitIntent` are now single
shared handles. Their exact pending identity, admission expectation, evaluated
graph, partition and conflict hashes, provenance identity, and semantic charge
remain immutable and continue to participate in semantic equality.

This does not add a trusted encoding or staging bypass. Transaction-current
validation, capacity reservation, sequence assignment, complete reciprocal
record-graph validation, canonical Protobuf preflight, backend candidate
matching, redb staging, and Immediate acknowledgement durability are unchanged.
Durable and public bytes are unchanged.

## Retain-or-revert evidence

The comparison point is WP-457's retained generated-client concurrency of 128.
All runs use the full 19,220-command seed over the public application gRPC
surface with PostgreSQL skipped. They are diagnostic single-host runs, not a
cross-backend parity claim.

| Build | Seed | Validation / encode / stage | Physical commits | Unary `create_comment` p50 |
|---|---:|---:|---:|---:|
| WP-457 retained | 3.767 s | 0.978 s | 342 | 2.077 ms |
| WP-458 run 1 | 3.542 s | 0.897 s | 342 | 2.729 ms |
| WP-458 run 2 | 3.498 s | 0.903 s | 343 | 1.740 ms |

The two WP-458 seeds improve by 6.0% and 7.1%, while the serialized staging
stage improves by 7.7% and 8.3%. The first unary sample was noisy; the repeated
sample is 16.2% faster than the WP-457 point. The change is retained because
both full seeds and both staging totals improve, the repeated unary measurement
does not reproduce a regression, and the semantic and backend suites remain
the release gate.

The remaining seed cost is still dominated by roughly 2.25 seconds of durable
flush time across about 343 physical commits. Further optimization should not
remove canonical preflight from publicly constructible Protobuf messages or
weaken the one complete durable acknowledgement boundary.
