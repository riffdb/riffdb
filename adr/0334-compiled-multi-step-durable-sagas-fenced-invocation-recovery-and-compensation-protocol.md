---
adr: "0334"
title: Compiled Multi-Step Durable Sagas Fenced Invocation Recovery and Compensation Protocol
status: accepted
tier: guarantee
date: 2026-11-05
accepted: 2026-11-05
acceptance: 'maintainer, in session, 2026-11-05: "approved, lets build" (ADR-0334 as written)'
requires: [ADR-0005, ADR-0109, ADR-0170, ADR-0252, ADR-0311]
amends: [ADR-0109]
supersedes: []
requirements: [WF-002, WF-005, WF-011, WF-012, WF-013, ID-001, ID-002, TXN-001]
packages: [WP-895, WP-896, WP-897, WP-898]
obligations: []
review_triggers:
  - "A saga step bypasses the ordinary compiled command runtime, authorization layer, or single commit coordinator."
  - "Saga step recovery reconstructs a different ADR-0005 IdempotencyIdentity across worker restarts or principal rotation."
  - "Forward or compensating progression proceeds while a prior step invocation has unknown commit status."
  - "An abort path after one or more effectful steps omits reverse compensation without an explicit terminal_uncompensated contract."
  - "Failed or authority-blocked compensation is reported as Compensated or rolled back via hidden storage mutation."
---
# ADR-0334: Compiled Multi-Step Durable Sagas Fenced Invocation Recovery and Compensation Protocol

## Context

`ADR-0109` and `ADR-0311` provide single-command workflow transitions, aggregate-local fenced leases, and a bounded scheduler library, while `.riffr` reaction modules provide single-hop event reactions. However, applications that coordinate work across multiple independent commands or partitions (such as reserving inventory in one partition, charging a customer balance in another, and confirming an order) must currently build an external orchestrator to persist step state, seal idempotency identities, resolve uncertain step outcomes after crashes, and run reverse compensations.

Because RiffDB forbids distributed two-phase commit locks (`AGENTS.md` Boundary 10) and requires every authoritative write to occur through an atomic compiled command (`Boundaries 2, 5, 6`), a built-in durable saga runtime must be defined as a **compiled orchestration state machine over ordinary commands** with an explicit two-phase invocation/checkpoint recovery protocol, byte-stable `ADR-0005` idempotency identities, honest compensation failure states, and bounded lifecycle management.

## Decision

1. **Public Guarantee and Contract-First `saga` Declaration:**
   - A RiffDB `saga` provides **durable multi-step command orchestration with independently committed steps and explicit compensation status**. Intermediate step commits are immediately observable in their target partitions; compensation is a semantic counter-command, never hidden transaction rollback; external side effects continue to use durable events and outbox reconciliation.
   - `.riff` contracts may declare `saga <Name>` with:
     - `partition_by <field>` (routing the coordinator state for the saga instance),
     - Typed `input (...)` (including caller `idempotency_key` and `saga_id: uuid`),
     - `execution_role <RoleName>` (a declared contract role whose capability closure covers every forward and compensating command referenced by the saga),
     - A bounded sequence of `1..=16` named `step <step_name>` blocks forming a forward-only DAG (no cycles or unbounded loops).
   - Each `step` names a target compiled `command`, pure input argument expressions derived from saga `input` and dominating step outcomes, exhaustive `on <OutcomeVariant> (effectful | no_effect) => next <step> | complete <SagaOutcome> | compensate <SagaOutcome> | abort_uncompensated <SagaOutcome>` branches, and an optional `compensate <Command>(...)` handler.

2. **Compile-Time Verification of Outcome Exhaustiveness and Compensation Coverage:**
   - Every outcome variant of a step's target command MUST be explicitly handled and classified as `effectful` (mutates state) or `no_effect` (zero-mutation rejection; verified by the compiler against the target command's outcome arm).
   - Every step that has at least one `effectful` outcome and can precede a `compensate <SagaOutcome>` transition MUST declare a `compensate <Command>(...)` handler.
   - Any branch after one or more potentially executed `effectful` steps that terminates without compensation MUST use the explicit keyword `abort_uncompensated <SagaOutcome>`; plain `abort` is rejected by the compiler once an `effectful` predecessor exists.

3. **Two-Phase Invocation / Checkpoint State Machine Across Partitions:**
   - Saga coordinator state is stored as an authoritative coordinator record (`StoredSagaInstanceV1`) in the saga's partition and advances strictly through fenced transitions (`saga_revision: u64` + `ADR-0109` worker lease fencing token):
     1. **Persist Next Invocation (`PrepareSagaStep`):** Before invoking any forward or compensating command, the worker commits a fenced transition at `saga_revision = r` recording `PendingStepInvocationV1`:
        - `step_index`, `direction` (`Forward | Compensate`),
        - Target `CommandId`, pinned `ContractLineage`, `ContractVersion`, and `CommandPlanHash`,
        - Canonical serialized command input bytes,
        - Exact `TenantScope`,
        - Stable delegated `ActorId`: `saga:<contract_lineage>:<saga_name>:<execution_role>`,
        - Deterministic caller idempotency key derived from `(saga_id, step_index, direction)`.
     2. **Submit or Resolve Ordinary Command:** The worker invokes (or resolves) the target command through the shared API-neutral application service using the exact fields of `PendingStepInvocationV1`. Because `DatabaseId`, `Environment`, `TenantScope`, `ActorId`, `ContractLineage`, `CommandId`, and caller key are fixed inside `PendingStepInvocationV1`, every retry or crash-recovery worker reconstructs the **exact byte-identical `ADR-0005` `IdempotencyIdentity`**, even if worker instances or operator credentials rotate.
     3. **Persist Outcome and Advance (`AdvanceSagaStep`):** Upon receiving a known terminal command outcome, the worker commits a fenced transition requiring `saga_revision == r` and the active lease fencing token, appending the outcome to the step ledger and advancing to `r + 1` (either preparing the next forward/compensating step or entering a terminal/halted saga status).
     4. **Crash / Uncertainty Barrier:** If the server crashes or times out between steps 1 and 3, the recovering worker reads `PendingStepInvocationV1` at revision `r`, resolves that exact `IdempotencyIdentity`, and repeats step 3. **Neither forward execution nor reverse compensation may advance while `PendingStepInvocationV1` has unknown commit status.**

4. **Reverse Compensation and Honest Non-Happy-Path Durable States:**
   - When a forward step transitions to `compensate <SagaOutcome>`, the coordinator walks backward through the instance's committed step ledger and schedules `compensate <Command>(...)` **only for steps whose committed forward outcome was `effectful`**, in strict reverse execution order.
   - Every saga instance is in one explicit `SagaLifecycleStatusV1`:
     - `RunningForward`: executing or preparing forward steps.
     - `Compensating`: executing or preparing reverse compensation steps.
     - `Completed`: terminal success on the forward path (`complete <SagaOutcome>`).
     - `AbortedClean`: terminal business abort before any `effectful` step committed.
     - `AbortedUncompensated`: terminal abort via an explicit `abort_uncompensated` branch.
     - `Compensated`: all executed `effectful` steps successfully completed their compensating commands.
     - `BlockedRetryable`: a forward or compensating command encountered transient contention, deadline, or unavailability; scheduled for bounded exponential backoff retry (`next_wakeup_at`).
     - `BlockedAuthority`: execution failed authentication, capability check, or row-policy authorization (e.g., revoked delegation); halted for operator/principal remediation and explicit `ResumeSaga`.
     - `CompensationFailed`: a compensating command returned a terminal non-success business outcome; halted with full step/compensation provenance and never reported as `Compensated`.

5. **Lifecycle, Pinning, Discovery, Bounds, and Operational Surfaces:**
   - **Authorization:** Starting a saga requires caller capability for the saga entrypoint and `execution_role`. Every step invocation re-evaluates current capability and row-policy rules (`WF-005`); lease possession never grants authority.
   - **Version Pinning:** Each `StoredSagaInstanceV1` pins `SagaPlanHash` and `ContractVersion` at admission. Compatible contract upgrades allow in-flight sagas to finish against their pinned command plans; retiring a referenced command version while active instances exist is rejected.
   - **Indexed Work Discovery & Fencing:** Non-terminal sagas are indexed by `(partition, lifecycle_status, next_wakeup_at, saga_id)` rather than a transient watermark, ensuring unfinished and backoff-delayed instances are revisited across restarts. The built-in `saga_worker` in `riffdb-server` claims instances with bounded `ADR-0109` fenced leases.
   - **Resource & Wait Bounds:** Step count `<= 16`; accumulated step-outcome state `<= 64 KiB` per instance; active non-terminal instances bounded per partition; automatic retry attempts bounded (`<= 8`).
   - **Client Wait vs. Cancellation:** `start_saga` and `await_saga` accept a bounded wait timeout (`<= 30s`) and return `SagaExecutionResult { saga_id, revision, status, terminal_outcome: Option<...> }` when the wait expires. Client disconnect detaches the wait without cancelling the saga; explicit `cancel_saga` is an authorized command that initiates reverse compensation once any in-flight `PendingStepInvocationV1` resolves.
   - **Backup, Restore, Changelog, and Export:** `StoredSagaInstanceV1` records participate in authoritative backups, restores, V3 changelog streams, and export snapshots.

## Options considered

1. **Cross-partition two-phase locking (2PC):** rejected by `AGENTS.md` Boundary 10 and because holding cross-partition locks across multi-step workflows degrades availability.
2. **Derive step idempotency from the ambient worker principal:** rejected because rotating worker credentials would alter the `ADR-0005` `IdempotencyIdentity` and risk duplicate step execution after a crash.
3. **Treat any compensation outcome as `Compensated`:** rejected because a compensating command can be rejected by business rules or revoked authority; hiding failed compensation violates interface safety.
4. **Two-phase fenced `PrepareSagaStep` / `AdvanceSagaStep` with pinned `PendingStepInvocationV1` and explicit compensation states:** chosen because every step remains an ordinary atomic command with exact crash replay and honest failure visibility.

## Consequences

- Multi-step cross-aggregate and cross-partition workflows execute durably inside `riffdbd` without external polling loops or 2PC locks.
- Each step incurs coordinator checkpoint commits (`PrepareSagaStep` and `AdvanceSagaStep`) in the saga's partition, trading two fast local transitions per step for exact crash recovery across partitions.
- Applications inspect explicit `Compensated`, `BlockedRetryable`, `BlockedAuthority`, and `CompensationFailed` states rather than assuming rollback.

## Standing design tests

- **Interface safety:** Callers cannot express unbounded loops, omit outcome branches, silently drop compensation after `effectful` steps, advance past an uncertain step, or confuse `CompensationFailed` with `Compensated`.
- **Scale:** Step count (`<= 16`), retained state (`<= 64 KiB`), worker batch claims, retry counts, and `await_saga` wait durations are strictly bounded and partition-local.

## Checks

- Compiler positive/negative corpus for step DAG bounds, outcome exhaustiveness, `effectful` vs `no_effect` classification, uncompensated abort rejection, and role capability closure.
- Deterministic crash-recovery tests injecting faults before `PrepareSagaStep`, during step execution, after step commit before `AdvanceSagaStep`, and during reverse compensation.
- Cross-partition saga execution, authority revocation (`BlockedAuthority`), failed compensation (`CompensationFailed`), backup/restore/export parity, and generated SDK/CLI/MCP tests.
