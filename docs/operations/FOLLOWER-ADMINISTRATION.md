# Follower registration and retirement

Operators can register and retire a follower through the primary's gRPC
`AdminService`, the Rust client, or `riffdb follower`. Both operations
use the shared application service and sole commit coordinator. They require a
current global `AdministerCapabilities` capability with all-partition scope.
Replication-stream permission alone is insufficient. Application roles,
row policies, tenant restrictions and unmet approval requirements refuse these
operations. They are not MCP tools.

Registration is available before contract deployment. The primary checks its
current database ID, history incarnation and leadership epoch. Followers refuse
both operations with the typed follower-mode error.

## Register a follower

Use the exact source identity and the same nonzero opaque 16-byte hold ID that
the follower's replication configuration will use. Obtain the source identity
from the validated source configuration or authenticated replication
observation. The examples use placeholder source identity and protected
operator configuration; replace them with the intended source values.

```bash
riffdb --config ./operator.toml --output json follower register \
  --database-id 018f22a1-7b3c-7def-8123-456789abcdef \
  --history-incarnation 1 --leadership-epoch 1 \
  --hold-id 71717171717171717171717171717171 \
  --hold-budget-sequences 100000
```

The sequence budget must be positive. An optional `--expires-at-sequence N`
selects a positive **application commit sequence**, not a duration or wall-clock
deadline. Reaching the budget degrades health and continues to retain history.
A configured expiry first persists degradation; a later bounded coordinator
turn can commit its audited release. See [replication health](REMOTE-INGRESS.md#replication-health-and-statistics).

A new registration retains the source frontier while awaiting bootstrap. It
can also adopt an existing legacy follower hold without advancing that hold's
fence. Registration does not install a follower, start replication, or prove
that it has caught up.

Success returns `administration_sequence`, `registration_generation`, and
`status` (`applied` or `replayed`). CLI sequences are decimal strings. Keep the
registration generation for retirement. The generation identifies this
registration; it is not an application commit sequence or an authorization token.

## Retry and retire

An exact registration retry uses the same lineage, hold ID, budget and expiry.
Each submission uses a fresh request ID; the CLI creates one automatically.
The retry returns the original receipt. A changed
policy refuses. Retrying after retirement returns historical registration
evidence and does **not** recreate the hold. Use a fresh hold ID for a new
registration. Retired identities remain tombstones and count toward the POC's
4096-entry source-hold bound.

Retire the exact generation returned by registration:

```bash
riffdb --config ./operator.toml --output json follower retire \
  --database-id 018f22a1-7b3c-7def-8123-456789abcdef \
  --history-incarnation 1 --leadership-epoch 1 \
  --hold-id 71717171717171717171717171717171 \
  --registration-generation 10
```

Replace `10` with the returned generation. Retirement releases this follower's
retention fence and matching bootstrap custody. Independent archive holds
remain. An exact retirement retry returns the original retirement receipt;
a stale generation refuses. A registration already released by configured
expiry cannot be reported as a successful explicit retirement.

The closed refusal results are `lineage_mismatch`, `registration_conflict`,
`registration_missing_or_stale`, `expiry_reached`, and `capacity_exhausted`.
Authentication, authorization, transport and uncertain-outcome errors use the
existing typed error surface. After an uncertain outcome, retry the identical
selection and policy with current authority. Changing identity would select a
different operation.

## Audit and POC limits

Started and terminal service audit records retain the exact lineage-scoped
follower target in `ServiceAuditRecordV3`. Success links the authoritative
registration or retirement receipt, including on replay. The hold, administration
record and V3 physical transaction commit atomically. Current authorization is
checked before admission, after capacity admission, and against transaction-
current facts before the coordinator commits.

Old audit generations retain their bytes. Use matching updated clients for the
new RPCs; see [compatibility](../compatibility.md). Primary fencing, authenticated
fence proof and promotion remain WP-748 work. Automatic registry migration is
not provided. Benchmark qualification remains paused; these operations do not
claim failover or performance qualification.
