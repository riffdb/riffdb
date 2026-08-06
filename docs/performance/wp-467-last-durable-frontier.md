# WP-467: Last-durable frontier and unpublished command state

WP-467 implements the storage safety boundary required by ADR-0098. It does
not enable durability-epoch selection in the public command coordinator yet.
The ordinary public path continues to use one Immediate redb commit per
physical command group.

## Closed storage protocol

A standard-profile epoch owns the mutation lease from start through its tail.
Each subgroup is still a complete command graph containing entity mutations,
the persisted outcome, commit record, provenance, durable events, outbox
intents, idempotency terminal, and linked service-audit lifecycle. The subgroup
may be applied with redb `Durability::None`, but storage retains its result in
an `UnpublishedAuditedBatchV1`. That state has no committed-result conversion
and never reaches a transport, notification sink, transient index, projection,
or subscriber.

The epoch is bounded across all of its subgroups by the existing 256-command
and 16 MiB semantic and encoded-write ceilings. The hardened profile rejects
the epoch protocol and keeps independently Immediate two-phase groups.

## Read visibility

Before the first deferred subgroup, storage captures the current durable redb
read transaction and publishes it as the epoch's read frontier. Operational
entity, index, idempotency, commit, event, outbox, projection, consumer, and
RiffQL reads use that immutable predecessor root while the epoch is active.
Writer-private transactions alone start from redb's newer root, so a later
subgroup sees earlier subgroup state in FIFO order.

Transient outbox indexes remain at the predecessor state until the tail.
Indexed reads do not wait on the epoch's writer lease; they use the retained
predecessor index state and the same durable read frontier.

The empty Immediate tail makes every preceding redb root durable. Only after a
known-successful tail does storage remove the predecessor frontier, apply the
accumulated transient-index deltas, and construct
`AuditedCommittedBatchV1` results in subgroup and command-sequence order.

## Failure behavior and evidence

Dropping an unfinished epoch, a deferred-commit error, or a tail error fences
authoritative writes. No result is released. An unknown tail keeps the old
frontier installed for the life of that process handle; reopening performs
normal redb recovery and exact RiffDB validation.

Automated coverage proves:

- all ordinary semantic read surfaces, including outbox/index-backed reads,
  see the predecessor state after a deferred subgroup;
- one successful tail publishes the complete command and audit graph;
- two independent subgroups remain invisible and then publish together in
  exact sequence order;
- a returned unknown tail status fences writes and retains the predecessor
  view, while reopen observes the complete durable epoch;
- process abort before and after the deferred subgroup recovers the preceding
  durable state; and
- process abort after the tail but before result release recovers the complete
  command graph.

The retained command-growth smoke on the recorded ext4/NVMe device measured
two 16-command non-durable engine groups plus one durable tail at 10.68 ms,
versus 31.41 ms for two independently Immediate one-phase groups (34.0%). The
device's same-run `fdatasync` p50 was 7.09 ms. This is engine-mechanics evidence
for the storage protocol, not a public coordinator result; no application
throughput claim is attached to WP-467.

WP-469 subsequently enabled the narrower production case justified by current
application evidence: one already-selected standard-profile physical group of
at least two audited commands may use one unpublished root followed immediately
by one Immediate tail. It adds no collection deadline and never combines
logical groups. Multi-subgroup epoch collection remains deferred.
