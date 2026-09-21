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
new RPCs; see [compatibility](../compatibility.md). Automatic registry migration
is not provided. The experimental fencing and promotion path below has recovery
and under-load proofs and a completed CI battery. Its
[implementation and fixture review](../architecture/WP-748-PROMOTION-IMPLEMENTATION-REVIEW.md)
was accepted and WP-748 completed on 2026-09-21. WP-750's release qualification
remains separate.

## Experimental fencing and promotion

WP-748 exposes `riffdb follower fence-primary` and `riffdb follower promote`
through the same checked Rust client, gRPC service and policy owner. Correctness
and human review are complete; this remains a POC path pending WP-750's release
qualification.

Fencing requires the distinct global, all-partition
`fence_replication_primary` permission and a currently attached registration.
It drains admitted commands, permanently refuses subsequent authoritative
writes as `RDB-REP-0102`, and survives restart. There is no unfence command.
Required audit and replication evidence remain available. A lost response
requires an exact retry with the same operation ID, follower and generation.

Promotion requires current global `AdministerCapabilities` authority. Point
operator configuration at the follower; retain the source fence operation and
registration selection. The daemon obtains proof through its configured
CA/name-verified TLS source connection. The caller cannot supply a new
incarnation, epoch, RPO or unauthenticated proof.

```text
riffdb --config ./source-operator.toml follower fence-primary \
  --database-id <SOURCE_DATABASE_UUIDV7> \
  --history-incarnation <INCARNATION> --leadership-epoch <EPOCH> \
  --hold-id <32_LOWERCASE_HEX_DIGITS> \
  --registration-generation <GENERATION> --operation-id <FENCE_UUIDV7>

riffdb --config ./follower-operator.toml follower promote \
  --database-id <SOURCE_DATABASE_UUIDV7> \
  --history-incarnation <INCARNATION> --leadership-epoch <EPOCH> \
  --hold-id <32_LOWERCASE_HEX_DIGITS> \
  --registration-generation <GENERATION> --operation-id <PROMOTION_UUIDV7> \
  --fence-operation-id <FENCE_UUIDV7>
```

Success reports the exact applied application sequence, new incarnation and
leadership epoch, administration sequence and application-sequence RPO.
Scoped causal tokens from the former incarnation return `RDB-HISTORY-0101`
on the promoted source, including while its local projection is unavailable.
Opaque continuations from the former service generation are invalid; restart a
query from fresh observations.
JSON integers are decimal strings; BeforeFirst is `null`. Keep both operation
IDs and the complete selection unchanged after an uncertain response. An exact
retry on a successfully promoted source checks fresh authority, returns the
original result and audits the invocation without contacting the former source.

Keep `<backup-root>/.maintenance/replication_promotion` with the database and
maintenance receipts. Missing or contradictory evidence fences readiness.
A failed or denied attempt that already froze a selection continues to fence
ordinary startup; terminal failure does not release its selected frontier.
Only complete reconciliation of an exact successful retry under the current
exclusive owner can discharge that selection. Earlier interrupted attempts for
that exact operation retain their original audit rows; a successful retry does
not fabricate terminal replies for them. Denial before selection does not leave
this fence. Never delete receipts to bypass recovery.

After committed cutover, restart validates the exact local control, audit and
external receipt before starting as a source, without loading the former
source's credentials. Pre-cutover restart fully validates the local follower and
hosts only authenticated `PromoteFollower` retries. It neither connects to the
source nor advances replication at boot, and emits no application-ready receipt.
Health, application reads/writes, bootstrap, replication and maintenance remain
unavailable. Use the same promotion operation, fence operation, target and
registration generation with a fresh request ID and current authority. Other
authenticated attempts are durably denied without changing the selection.

An authorized retry requires the configured source to be available for fresh
authenticated fence proof. It drains local readers, rechecks current capability
facts and expiry, and preserves the selected frontier before cutover. Success
is returned only after complete source validation and service activation. If
interrupted, retain the same configuration and evidence and retry; shutdown
does not imply that an uncertain cutover failed.
Follower projection generation and lifecycle remain locally owned derived
state under ADR-0248; authoritative commit/entity zero-RPO claims do not extend
to derived state.
