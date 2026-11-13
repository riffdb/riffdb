---
adr: "0349"
title: Deferred-pipeline idempotency disjointness, commit-check rejection classification, and CLI bootstrap/workspace ergonomics
status: accepted
tier: guarantee
date: 2026-11-12
accepted: 2026-11-12
acceptance: 'maintainer, in session, 2026-11-12: "yes"'
requires: ["0005", "0011", "0023", "0052", "0156", "0200", "0305", "0348"]
amends: ["0011", "0023", "0052", "0200", "0305"]
supersedes: []
requirements: ["TXN-001", "TXN-002", "PERF-022", "CMP-001", "AAA-004", "DX-003", "DX-004", "DX-006", "HBK-003"]
packages: ["WP-928"]
obligations:
  - id: OBL-0349-1
    package: WP-928
    description: "Deferred-pipeline footprints include candidate idempotency identities so duplicate idempotency submissions across in-flight writer units or within a command group drain prior completions and evaluate on the published frontier without failing recheck or stopping the coordinator"
    proof: concurrent_duplicate_idempotent_commands_across_deferred_pipeline_replay_without_stopping_coordinator
  - id: OBL-0349-2
    package: WP-928
    description: "CandidateValidationRejection::CommitCheckRejected after attempt-budget exhaustion surfaces as CommandExecutionErrorKind::CommitCheckRejected and maps to validation_failed (RDB-INPUT-0101, recovery correct_request) while remaining non-durable in idempotency storage"
    proof: commit_check_rejected_maps_to_validation_error_instead_of_deadline_exceeded
  - id: OBL-0349-3
    package: WP-928
    description: "CLI capability bootstrap removes newly created --bearer-output files on definitive failure and diagnoses loopback/bootstrap-key requirements on authentication_failed; workspace_lock_path accepts lock paths outside cwd that resolve inside the application root; RDB-AS011 names the missing member; riffdb dev isolates projections under its work directory; and CLI output ignores EPIPE on closed stdout"
    proof: failed_bootstrap_cleans_up_bearer_output_and_workspace_lock_resolves_relative_to_caller
review_triggers:
  - "Deferred writer-private pipeline admits two commands or in-flight units sharing an IdempotencyIdentity without draining prior completions."
  - "CommitCheckRejected writes a durable StoredExecutionFailedV1 outcome instead of leaving the idempotency row non-terminal."
  - "workspace_lock_path permits a lockfile whose canonical parent escapes the canonical application root."
---
# ADR-0349: Deferred-pipeline idempotency disjointness, commit-check rejection classification, and CLI bootstrap/workspace ergonomics

## Context

End-to-end multi-service testing of RiffDB and its adapters surfaced five defects:

1. **Concurrent duplicate idempotent commands under `CoordinatorDurability::Group` stop the database (`storage_unavailable`):**
   - Under `CoordinatorDurability::Group`, a `Vacant` terminal command admission (`terminal_admission == true`, such as a newly evaluated `Committed` or `Rejected` command) stages its `StoredOutcomeV1` in the unpublished deferred epoch in `in_flight` without writing an intermediate `Pending` row to the published redb root.
   - If a duplicate submission carrying the same `IdempotencyIdentity` arrives in a subsequent `WorkUnit::CommandGroup` while the first unit is still in `in_flight` (or if two commands in the same group share an `IdempotencyIdentity`), `reduce_audited_command_admission_group` reads the published redb root (`NotFound` -> `Vacant`).
   - Because `DeferredPipelineFootprint` and `CompatibleCommandGroup` tracked only entity `AccessSummary` conflicts and not `IdempotencyIdentity` sets, `can_pipeline` admitted the duplicate onto `CommandEvaluationFrontier::WriterPrivate`, where either `execute_compatible_command_group_in_writer_private_tx` rejected non-unique `serial_members_eligible` or `recheck_admission` observed the unpublished outcome from the prior epoch as an unexpected state mismatch and called `lifecycle.stop()`, taking the database offline until server restart.
2. **Broken entity/aggregate `invariant` (`CommitCheckRejected`) reported as `RDB-APP-0003` (`deadline_exceeded`):**
   - Per `SPEC.md` and ADR-0023, `CandidateValidationRejection::CommitCheckRejected` (a post-mutation entity or aggregate invariant violation) rolls back the candidate and consumes non-durable attempt slots up to `MAX_COMMAND_EVALUATION_ATTEMPTS_V1 = 3` without persisting `StoredExecutionFailedV1`.
   - When all three attempts rejected with `CommitCheckRejected`, `PendingCommandAttempts` returned `CommandAttemptError::RetryBudgetExhausted`, which `crates/riffdb-service` mapped to `ControlError::DeadlineExceeded` (`RDB-APP-0003`, `"application request deadline elapsed"`, recovery `"retry"`), misleading callers and SDK retry loops into retrying a deterministic invariant violation.
3. **Failed remote operator bootstrap leaves a stale `--bearer-output` file:**
   - `BootstrapMaterial::retain` wrote `--bearer-output` before invoking `CreateCapability`. When the RPC failed definitively (for example `Unauthenticated` when called from a non-loopback address), `BootstrapMaterial::abandon` deleted only the automatic `~/.config/riffdb/bootstrap/...` file and left the 43-byte `--bearer-output` file on disk, and the CLI error did not explain that `capability bootstrap` requires a loopback connection (`127.0.0.1` / `::1`) and matching bootstrap capability keys.
4. **Application workspace lock path, `RDB-AS011` diagnostic detail, `riffdb dev` projection directory, and CLI `EPIPE`:**
   - `workspace_lock_path` in `crates/riffdb-cli/src/scaffold.rs` required `--lock` to be a single relative filename, rejecting `riffdb application deploy /path/to/app/riffdb.application.json --lock /path/to/app/riffdb.application.lock.json` when invoked outside `/path/to/app` (`RDB-FS006`).
   - `ApplicationSourceError::MissingRequiredEntry` (`RDB-AS011`) omitted the member path when `array()` checked minimum length, and authoring/CLI diagnostics did not include `error.member()`.
   - `riffdb dev` did not pass `--projections-root` to `riffdbd`, causing `riffdbd` to create `./projections/` in the user's current working directory instead of the `riffdb dev` work directory.
   - `Terminal::emit` in `crates/riffdb-cli` used `println!` / `print!`, panicking on `BrokenPipe` when piped to `head`.

## Decision

1. **Idempotency-identity disjointness in `DeferredPipelineFootprint` and `CompatibleCommandGroup` (`crates/riffdb-idempotency`, `crates/riffdb-commit`):**
   - Expose `PreparedIdempotencyRecheckV1::lookup_candidates(&self) -> &IdempotencyLookupCandidatesV1` and include `idempotency_identities: Vec<IdempotencyIdentity>` in `PreparedCommandCompatibility`.
   - Track `identities: BTreeSet<IdempotencyIdentity>` in both `DeferredPipelineFootprint` and `CompatibleCommandGroup`.
   - Reject intra-group duplicate idempotency identities in `CompatibleCommandGroup::try_insert`, `try_insert_prepared_pipeline`, and `command_group_deferred_pipeline_footprint`.
   - Store `footprint: Option<DeferredPipelineFootprint>` on `InFlightWriterUnit` in `crates/riffdb-commit/src/audit_executor.rs` and require candidate deferred-pipeline units to be idempotency-disjoint from every `in_flight` unit (`!prior.shares_idempotency_identity(candidate)`). When a duplicate identity arrives while a prior unit is in `in_flight`, the writer drains `in_flight` first and evaluates on `CommandEvaluationFrontier::Published`, where `reduce_audited_command_admission_group` observes the committed `StoredOutcomeV1` and replays cleanly.
2. **Distinct `CommitCheckRejected` classification (`crates/riffdb-commit`, `crates/riffdb-service`, `docs/`):**
   - Track `last_rejection: Option<CandidateValidationRejection>` on `PendingCommandCore`. When attempt budget is exhausted after `CandidateValidationRejection::CommitCheckRejected`, surface `CommandAttemptError::CommitCheckRejected` and `CommandExecutionErrorKind::CommitCheckRejected` while keeping the idempotency row non-durable (`Pending`, byte-identical) per ADR-0023.
   - Map `CommandExecutionErrorKind::CommitCheckRejected` in `crates/riffdb-service/src/command_operations.rs` to `PublicError::validation(ValidationIssues::one(ValidationIssue::new(ValidationCode::InvalidValue, ValidationPath::root())))`, which surfaces on the application protocol as `RDB-INPUT-0101` (`"application input is invalid"`, category `"input"`, recovery `"correct_request"`).
3. **CLI bootstrap cleanup, workspace `--lock` resolution, `RDB-AS011` member detail, `riffdb dev` projections path, and `EPIPE` handling (`crates/riffdb-cli`, `crates/riffdb-query-module`, `crates/riffdb-diagnostics`):**
   - Track newly created `--bearer-output` files in `BootstrapMaterial` and remove them in `BootstrapMaterial::abandon` on definitive non-`OutcomeUnknown` bootstrap failures; append a loopback/bootstrap-key hint when `capability bootstrap` fails with `authentication_failed`.
   - Update `workspace_lock_path` so multi-segment relative or absolute `--lock` paths resolve against the caller's working directory and succeed whenever their canonical parent equals the canonical application root.
   - Attach `.at(member)` to `ApplicationSourceError::MissingRequiredEntry` and `LimitExceeded` in `array()` and include `error.member()` in `RDB-AS011` diagnostic details.
   - Pass `--projections-root <work_root>/projections` when `riffdb dev` spawns `riffdbd`.
   - Ignore `io::ErrorKind::BrokenPipe` when writing CLI output to `stdout`.

## Options considered

- **Persisting `Pending` before deferred group commit for all terminal admissions:** Rejected because writing an extra synchronous transaction before group commit would defeat group-commit batching for non-conflicting commands; draining `in_flight` only when `IdempotencyIdentity` overlaps preserves full group-commit throughput while guaranteeing published-frontier replay for concurrent duplicates.
- **Adding a new protobuf error code for `CommitCheckRejected`:** Rejected because `CommitCheckRejected` is a post-mutation input/state invariant failure and `RDB-INPUT-0101` (`validation_failed` / `invalid_value`, recovery `correct_request`) already represents invalid command input on the stable wire protocol without breaking `.proto` compatibility.

## Consequences

- Concurrent duplicate idempotent commands under `CoordinatorDurability::Group` never enter `WriterPrivate` alongside an in-flight unit with the same `IdempotencyIdentity`; one commits and all duplicates replay the stored outcome without stopping the coordinator.
- Broken entity/aggregate `invariant` checks return `RDB-INPUT-0101` (`correct_request`) instead of `RDB-APP-0003` (`retry`).
- Failed `capability bootstrap --bearer-output` calls leave no unusable bearer file behind.

## Standing design tests

- **Interface safety:** `CommitCheckRejected` remains non-durable in idempotency storage (`Pending` row unchanged) while surfacing a non-retryable `input` category error (`RDB-INPUT-0101`); `workspace_lock_path` still rejects any lockfile outside the canonical application workspace root or behind a symlink.
- **Scale:** `DeferredPipelineFootprint.identities` contains at most `MAX_GROUP_SIZE * 2` fixed-size 32-byte `IdempotencyIdentity` entries per unit.

## Checks

- Proof tests `concurrent_duplicate_idempotent_commands_across_deferred_pipeline_replay_without_stopping_coordinator`, `commit_check_rejected_maps_to_validation_error_instead_of_deadline_exceeded`, and `failed_bootstrap_cleans_up_bearer_output_and_workspace_lock_resolves_relative_to_caller`.
- `./scripts/check-adr-obligations`, `./scripts/check-acceptance-commits`, `./scripts/check-generated`, and `./scripts/handbook check`.
