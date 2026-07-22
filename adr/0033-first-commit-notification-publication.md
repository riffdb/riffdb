# ADR-0033: First-Commit Notification Publication

- **Status:** Accepted
- **Direction approved:** 2026-07-21
- **Exact text accepted:** 2026-07-21
- **Accepted:** 2026-07-21
- **Requires:** ADR-0003, ADR-0004, ADR-0005, ADR-0007, ADR-0012,
  ADR-0023, and ADR-0028
- **Amends:** ADR-0007 commit-notification source composition and ADR-0023
  coordinator completion edge
- **Decision deadline:** Before WP-130 composes `riffdbd`

The human maintainer approved this decision and its authoritative-file
amendments on 2026-07-21. This record repairs a missing process-local
post-durability handoff. It changes no public Protobuf field, durable record,
storage key, command result, commit sequence, or service operation.

## Context

The specification requires the commit coordinator to publish bounded
post-durability notifications and requires `SubscribeCommits` to use the shared
service with a 128-subscriber, 256-item-per-subscriber process-local source.
`riffdb-service` already owns the policy-filtered catch-up/live subscription
algorithm and its consumer port, while WP-130 owns the production implementation
of that port.

The completed coordinator returned durable outcomes to callers but exposed no
least-authority way to tell the server-owned live source that a new application
commit had become durable. Polling storage would weaken the required publication
edge, give no exact wakeup ownership, and complicate the race between catch-up
and live delivery. Giving the server a repository or commit-transaction handle
would violate the sole-writer and API-neutral service boundaries.

## Decision

### Coordinator-owned publication edge

`riffdb-commit` owns a synchronous, object-safe
`ApplicationCommitNotificationSink`, or a mechanically equivalent name, whose
only successful publication input is one `CommitSequence`. Publication returns
a closed, redaction-safe infrastructure result and exposes no repository,
transaction, commit record, outcome, event, entity key, tenant, actor, or
business value.

Production `RunningCommandCoordinator::start` requires an explicit injected
sink. It has no default or backend-inferred production path. Test harnesses may
inject local recording or discard sinks, but `riffdbd` must inject the one
server-owned bounded notification hub consumed by its
`AuthoritativeReadPort` implementation.

After command execution has released transaction and logical capabilities, the
coordinator examines the closed result. It invokes the sink exactly once only
when the current invocation returned `CommittedOutcomeDisposition::FirstCommit`.
The sequence comes from that exact durable `StoredOutcomeV1`. It never publishes
for:

- equal-input terminal replay or outcome resolution;
- read-only execution;
- execution-failure terminalization, preparation change, input mismatch, or any
  error;
- catalog deployment, database initialization, capability administration, or
  service/administration audit; or
- any state that did not produce the complete authoritative application commit
  graph.

The sink call occurs only after durable success and before the actor releases
the current completion to its waiting caller. A sink error or panic is contained
at this boundary. It cannot change, hide, or roll back the already-known durable
command result: that result is still released to its current caller. The
coordinator publishes `Stopped`, closes admission, and rejects queued and future
work with the existing stopped semantics. WP-130 supervision must then stop new
routing and authoritative readiness. No retry is attempted because duplicating
process-local hints is unnecessary and authoritative catch-up remains available
after a healthy restart.

### Server-owned bounded hub

WP-130 owns one process-local hub that implements both the injected commit sink
and the lower notification source consumed through the service-owned
`AuthoritativeReadPort`. The hub is not storage and never constructs a public
notification. It retains at most 128 subscribers and exactly 256 pending
sequence hints per subscriber.

Each subscriber records its last safely delivered sequence. When its buffer
would overflow, the hub closes that subscriber instead of dropping or replacing
a hint. The service maps closure to the accepted typed lagged terminal carrying
only that last safe resume position. Catch-up scans remain authoritative and
bounded to 500 commits; hints merely trigger or feed rereads. The shared service
still reloads current policy and applies redaction before every visible commit.
A stale or duplicate sequence is harmless and cannot itself release data.

Subscriber installation occurs before its catch-up scan. The service joins the
bounded catch-up and queued live hints by sequence, discards already-covered
hints, and requires contiguous authoritative rereads. A gap, source closure,
policy change, cancellation, deadline, or server shutdown uses the existing
typed terminal semantics rather than silent loss.

## Options Considered

1. **Coordinator-owned sequence-only sink into a server-owned bounded hub:**
   Accepted. It preserves the exact durability edge and least authority.
2. **Poll redb for the latest sequence:** Rejected. It does not implement the
   required coordinator publication edge and introduces avoidable latency and
   race ambiguity.
3. **Publish complete commit records or outcomes:** Rejected. It duplicates
   authoritative state outside storage and bypasses service policy/redaction.
4. **Let gRPC or the service observe coordinator receipts:** Rejected. Dropped
   callers, MCP, SDK, and internal invocations would then produce inconsistent
   subscription behavior.
5. **Silently ignore sink failure or overflow:** Rejected. That could create an
   apparently healthy process that silently loses required wakeups.

## Consequences

- WP-100 receives a narrow constructor/interface correction and first-commit
  publication tests.
- WP-130 supplies and supervises the bounded hub and adapts it through the
  existing API-neutral service port.
- The coordinator remains the only owner of sequence assignment and durable
  command application; the hub receives only an already-assigned sequence.
- Storage remains authoritative, so process restart or a lagged subscriber
  resumes by bounded commit scan rather than replaying in-memory data.

## Compatibility

This changes an internal Rust coordinator constructor and process-local
composition only. It changes no Protobuf package, message, field, enum, RPC,
canonical encoding, durable envelope, storage key, IR, plan hash, input hash,
idempotency identity, commit order, or public error.

## Security

The sink carries no business data or authorization evidence. A sequence hint
cannot authorize disclosure; the shared service performs current-policy checks
and redaction against the authoritative commit. Sink errors and panics are
contained without logging payloads and fail readiness closed.

## Testing

WP-100 proves exactly-once process-local publication for first durable commit,
no publication for replay/read-only/failure/control-plane results, publication
after durability and capability release, and contained sink error/panic that
preserves the known result while stopping admission.

WP-130 proves the 128-subscriber and 256-item bounds, installation-before-scan,
contiguous catch-up/live joining, stale/duplicate hints, lagged last-safe resume,
subscriber cancellation and shutdown, and supervision failure. WP-200 provides
the final public stream, policy, restart, and recovery evidence.

## Requirements and Work Packages

- **Requirements:** `SYS-004`, `TXN-040`, `LOG-001`, `API-001`, `POC-005`
- **Corrects:** `WP-100`
- **Blocks:** `WP-130`
- **Consumed by:** `WP-120`, `WP-130`, `WP-140`, and `WP-200`
- **Final evidence:** `WP-200`
