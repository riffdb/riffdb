---
adr: "0205"
title: Live Command Fresh-Locator Arming and Audit Cause Preservation
status: accepted
tier: guarantee
date: 2026-09-05
accepted: 2026-09-06
requires: [ADR-0070, ADR-0100, ADR-0104, ADR-0165, ADR-0197]
amends: [ADR-0197]
supersedes: []
requirements: [OUT-001, OUT-002, TXN-042, REC-004, PERF-019]
packages: [WP-705]
obligations:
  - id: OBL-0205-1
    package: WP-705
    proof: production_grouped_command_arms_fresh_locator_coverage_without_manual_arm
    says: A real production grouped coordinator executes the first fresh command through the live fused transaction path and every later new-key inspection uses exact public coverage with zero fallback history scans, without a test or caller manually arming it.
  - id: OBL-0205-2
    package: WP-705
    proof: production_fresh_locator_arming_site_is_reachable_from_both_command_batch_paths
    says: Architecture checks prove the sole production arming call is in the shared fused command transaction constructor reached by both direct and deferred command batch paths before command staging.
  - id: OBL-0205-3
    package: WP-705
    proof: request_scoped_command_terminal_audit_failure_preserves_threshold
    says: Command success and failure completion preserve a request-scoped terminal-audit append cause, fail the affected invocation closed, and leave global readiness up until the unchanged eighth consecutive request-scoped failure.
review_triggers:
  - Coverage would arm anywhere except BatchCore::open_with_access, before mutation-gated write access exists, after command staging or mutation begins, more than once, or from nonempty, checkpoint, cardinality, restart, or caller evidence.
  - Fused terminal admission would persist Pending, call the dormant admission-write path, separate Started from the command transaction, or change transaction ordering, conflict ownership, acknowledgement, outcome sequencing, or publication visibility.
  - Any existing ADR-0197 private/public witness, epoch, locator validation, preservation, publication, rebase, disablement, uncertainty, restart, bound, or durable-byte rule would change.
  - The production proof would manually arm coverage, bypass the grouped coordinator, omit either direct or deferred reachability, or fail to assert zero later fallback scans.
  - An audit failure cause would be inferred from the command result instead of preserved from AuditAppendFailure, or the immediate subsystem stop, success reset, consecutive count, threshold of eight, deadline, cancellation, retry, or transport behavior would change.
  - Implementation would touch a path other than the exact six paths named in Decision 9, let the bounded service-test barrier affect production or scheduling semantics, or add any public, protocol, storage-key, durable-format, configuration, or operator surface.
---
# ADR-0205: Live Command Fresh-Locator Arming and Audit Cause Preservation

## Context

ADR-0197 correctly requires one exact-empty proof at the first mutation-gated
command write, but names `RedbOperationalPorts::admit_or_resolve_group` as its
sole initialization site. The current fused terminal-admission command path
deliberately never calls that method: it returns a vacant execution candidate,
opens a command batch, and atomically publishes Started, terminal outcome, and
command authority without a durable Pending row. The accepted obligation test
manually invokes the arming helper, so it proves the state machine but not the
production call graph. In a real fresh process the state remains
`Uninitialized`, and each new-key operational inspection repeats the retained
fallback history scan until request deadlines fail.

That failure exposed a separate implementation mismatch with ADR-0070. A
terminal audit append returns a closed `RequestScoped | SubsystemUnavailable`
cause, but command success and failure finish paths currently discard it and
invoke the unconditional subsystem handler. A caller deadline can therefore
stop global readiness after one affected command instead of the accepted eighth
consecutive request-scoped failure. This record authorizes only the reachability
correction and preservation of that already accepted cause.

## Decision

1. ADR-0197 Decision 1 is amended only as follows. The sole process-lifetime
   initialization opportunity is `BatchCore::open_with_access`, after its
   caller has acquired mutation-gated `RedbWriteAccess` and before it reads the
   allocator or performs any command staging or mutation. Both
   `RedbOperationalPorts::begin_empty_batch` and
   `RedbDurabilityEpoch::begin_empty_batch` reach this same constructor, so
   direct and deferred grouped command paths share one initialization edge.
   `RedbOperationalPorts::admit_or_resolve_group` no longer arms coverage.

2. The first command's read-only pre-admission inspection remains before this
   edge and retains its existing empty-history result. At this sole production
   call site the live command transaction supplies its write access to the
   existing exact-empty arming primitive. That primitive's `Uninitialized`
   guard permits only one process-lifetime arming attempt; the exact ADR-0197
   Decision 2 frontier, authority
   tables, allocator, transient-index, retention, and fence conditions remain
   conjunctive. Any false fact, read failure, poison, or contradiction sets the
   process-lifetime state to `Disabled` under the existing rules.

3. The constructor does not create, persist, reconstruct, copy, or expose a
   proof. It only supplies the existing writer-private transaction to the
   existing arming primitive. Every ADR-0197 Decision 3 through 14 rule for
   fixed-size roles, exact successor chains, locator and capsule validation,
   publication FIFO, preserving Immediate lanes, direct publication, rebase,
   failure, uncertainty, restart, bounds, and loss-to-Disabled remains exact.

4. Fused terminal admission remains unchanged. A vacant preparation creates no
   durable Pending row and does not call `AdmissionRepository::admit_or_resolve`
   or `admit_or_resolve_group`. Started audit, terminal outcome, command state,
   events, provenance, outbox intent, allocators, locators, and commit record
   retain their existing atomic command transaction and acknowledgement edge.
   The arming check itself performs no authoritative mutation.

5. A deterministic semantic proof opens an exact fresh redb database and sends
   distinct commands through the real production grouped coordinator. It does
   not call an arming method, test-only arming hook, storage batch, or repository
   admission method directly. The first command must commit normally; at least
   one later new-key preinspection and transaction-adjacent recheck must return
   the unchanged absence result with the fresh-locator fallback-scan counter
   still exactly zero. Replays and input mismatch retain their existing results.

6. A source architecture proof identifies exactly one production call to the
   arming primitive, inside `BatchCore::open_with_access`, proves both direct and
   deferred command batch constructors reach it, and proves no test helper,
   dormant admission-write method, public audit, subscription, checkpoint,
   startup, maintenance, or caller-controlled path can reach it. It also freezes
   arming before allocator read and all staging/mutation calls.

7. When `BegunInvocation::finish` returns `AuditAppendFailure`, command success
   and failure completion pass that exact `cause()` to
   `note_audit_failure_with_cause`; they do not replace it with an inferred or
   unconditional subsystem cause. The affected invocation still withholds its
   output and retains the existing `storage_unavailable` or `outcome_unknown`
   mapping according to its durable knowledge. Capacity-overload preservation
   remains unchanged.

8. ADR-0070's audit-readiness rule is not amended: subsystem unavailability
   stops routing immediately; deadline, cancellation, and capacity remain
   request-scoped; a successful audit receipt resets the consecutive counter;
   the eighth consecutive request-scoped failure stops routing permanently.
   This record changes no deadline duration, scheduling, cancellation,
   admission cap, batch size, concurrency, retry budget, re-entry, gRPC status,
   health recovery, or threshold.

9. WP-705 implementation authority is limited to exactly these paths:
   `crates/riffdb-storage-redb/src/application.rs`,
   `crates/riffdb-storage-redb/tests/architecture.rs`,
   `tests/command_semantics/command_concurrency.rs`,
   `crates/riffdb-service/src/command_operations.rs`, and
   `tests/service/service_audit_orchestration.rs`, plus
   `tests/service/support/mod.rs` solely for a bounded deterministic one-shot
   final-command-reauthorization barrier and capacity-holding helper that lets
   the runtime proof drive `RequestScoped` terminal-audit failure through both
   `finish_success` and `finish_failure`. The helper remains test-only and may
   not add a production hook or public surface or change command or audit
   scheduling, deadlines, cancellation, capacity, retry, or threshold
   semantics. A required edit elsewhere stops the package for another exact
   human-reviewed amendment.

10. WP-705 reruns its unchanged production lifecycle and 65,536-row evidence
    only after these semantic and architecture proofs pass. Evidence collected
    with manual arming, a changed timeout or concurrency, a skipped audit, a
    disabled fallback assertion, or a weakened threshold is ineligible. This
    proposed record authorizes no implementation until a human accepts its exact
    text.

## Options considered

1. **Call the dormant admission-write method from production:** rejected; it
   would reintroduce durable Pending and change fused audit and command ordering.
2. **Arm before read-only idempotency inspection:** rejected; that point owns no
   mutation-gated writer-private transaction and cannot prove exact authority.
3. **Arm in each direct and deferred constructor separately:** rejected; two
   production sites weaken the one-time reachability and architecture proof.
4. **Raise deadlines, lower concurrency, or weaken audit readiness:** rejected;
   those hide the unreachable proof and alter accepted failure semantics.
5. **Use the shared live transaction constructor and preserve the returned audit
   cause:** selected as the narrow correction to the two accepted guarantees.

## Consequences

- Fresh-process command writes can carry ADR-0197 coverage through the actual
  fused publication path, eliminating repeated fallback scans after the first
  command without adding authority or durable state.
- The first fresh command retains one fixed exact-empty arming check; restarted
  or nonempty databases retain the existing fallback and Disabled behavior.
- Caller-scoped audit pressure can no longer masquerade as immediate subsystem
  loss, while every affected invocation and the unchanged eighth-failure
  threshold remain fail-closed.
- General idempotency optimization, checkpoint policy, batch redesign, timeout
  tuning, audit recovery, and changes outside the six paths remain deferred.

## Standing design tests

- **Interface safety:** arming remains automatic and storage-private; no
  application, agent, operator, transport, or configuration can request,
  inspect, preserve, reset, or bypass it or weaken audit readiness. The
  one-shot reauthorization barrier and capacity helper exist only in the
  integration-test harness and create no production hook or public control.
- **Scale:** one fixed proof and existing bounded witnesses replace repeated
  history scans; no population-proportional state is retained, and all command,
  journal, audit, and batch bounds remain unchanged.

## Checks

- `production_grouped_command_arms_fresh_locator_coverage_without_manual_arm`
  proves the real grouped coordinator path and zero later fallback scans.
- `production_fresh_locator_arming_site_is_reachable_from_both_command_batch_paths`
  freezes the sole live call site, direct/deferred reachability, and ordering.
- `request_scoped_command_terminal_audit_failure_preserves_threshold` proves
  both command finish paths under the bounded one-shot barrier, affected-command
  fail-closure, success reset, immediate subsystem stop, and no global
  request-scoped stop before the unchanged eighth consecutive failure.
- Existing ADR-0197 state-machine, locator, publication, rebase, restart,
  corruption, and cold-fresh proofs remain unchanged and pass together.
