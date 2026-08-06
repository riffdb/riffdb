# WP-469: Fenced single-subgroup standard commit

WP-469 enables ADR-0098's unpublished-root protocol for one already-selected
public command group. It changes a redb physical commit mechanism, not RiffDB's
logical command or acknowledgement semantics.

## Retained selection rule

The coordinator selects the fenced path only when all of these facts are true:

- the server uses the standard `Group` durability profile;
- compatibility planning has already selected one physical group containing at
  least two commands;
- every command carries its complete checked application-service audit
  lifecycle; and
- the group remains inside the existing 256-command and 16 MiB storage bounds.

The coordinator adds no wait, timer, queue drain, compatibility merge, or new
transaction boundary to form this candidate. An idle or contended singleton
still commits directly with one one-phase Immediate redb transaction. The
hardened `Sync` profile still commits each selected group directly with redb's
two-phase Immediate mechanism. Unaudited internal harnesses also remain direct.

## Publication and failure boundary

The complete command group is first applied to a redb transaction using
`Durability::None`. That transaction contains every command's entity and index
mutations, persisted outcome, idempotency terminal, commit record, provenance,
domain events, outbox intents, and Started plus terminal service-audit records.
The storage epoch retains move-only unpublished results that cannot be converted
to ordinary committed results.

An empty one-phase Immediate transaction follows without releasing the writer
lease. redb's ordered durability makes that tail a fence for the earlier root.
Only after the tail is known successful does storage remove the predecessor
read frontier, update transient indexes, construct committed results, publish
notifications, and release public responses.

Operational and derived readers therefore see the predecessor frontier until
the fence succeeds, then the complete successor. A deferred apply or tail error
releases no result and is treated as commit-status unknown. The coordinator
performs bounded exact identity resolution; contradictory or unavailable
resolution stops authoritative admission. Dropping an unfinished redb epoch
also fences that process handle. Reopen uses normal redb recovery and RiffDB's
structural validation.

Each command retains an independent identity, sequence, declared outcome,
audit, provenance, event set, replay result, and uncertainty resolution. The
mechanism is not a public batch transaction and does not create shared success
or rollback semantics between commands.

## Automated evidence

The production-path redb test forces a singleton followed by a two-command
audited standard group and observes these engine boundaries in order:

1. `CommandBatch` for the singleton;
2. `DeferredCommandBatch` for the non-singleton candidate root; and
3. `CommandEpochTail` before either grouped result is returned.

A parallel hardened test observes only direct `CommandBatch` boundaries. The
memory semantic backend proves the candidate entity state cannot be read before
`fence()` and that the complete two-command result appears after it. Existing
redb process-recovery coverage exercises aborts before and after the deferred
root and response loss after the tail.

## Performance evidence

A same-process redb mechanics experiment applied the same 32 complete synthetic
command graphs through the direct and fenced mechanisms on the retained
ext4/NVMe host:

| Physical mechanism | Elapsed |
| --- | ---: |
| One one-phase Immediate group of 32 | 10.229 ms |
| One non-durable group of 32 plus Immediate tail | 6.987 ms |

The fenced mechanism was 31.7% faster in that isolated engine measurement.

The full generated-Rust/gRPC TicketDesk seed then executed 19,220 ordinary
commands at concurrency 384. Three adjacent iterations completed in 2.470,
2.490, and 2.482 seconds; the reported median was 2.482 seconds, or 7,747
commands/s. The prior WP-468 median was 2.478 seconds. This is effectively
neutral end-to-end, not a seed-speed claim. The representative writer traces
reported about 1.28 seconds in commit/flush work, while validation, encoding,
staging, admission, and queue residence consumed the remaining critical path.

WP-469 is retained because it narrows physical durability work without a unary
regression and establishes the production safety boundary needed by future
epoch work. Further seed improvement must target the now-dominant command
pipeline CPU and queueing costs. Multi-subgroup collection remains unimplemented
until evidence justifies its additional scheduler and recovery surface.

These measurements are short same-host engineering evidence, not a published
cross-database comparison.
