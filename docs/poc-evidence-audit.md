# POC evidence audit (WP-200)

Package: WP-200. Date: 2026-09-18. Source revision at audit:
`2a30b2bd05616db6a07feb333142fe94fdde7729`.

This sizes remaining WP-200 work. It does not sign off POC-001 through
POC-010. Claims below name an artifact or they do not count. The candidate
manifest `release/evidence/poc-requirements-v1.json` is a pointer list with
`attestation: candidate_not_executed` and every requirement
`status: requires_execution`. It is not passing evidence. This audit does
not edit that file.

Method: SPEC.md §2.2 text, the candidate manifest, `scripts/demo`,
`scripts/release-poc`, `scripts/release-evidence`, recovery fixtures, and
the tests those documents name. No `req: POC-*` tag exists under `crates/`,
`tests/`, `clients/`, `examples/`, `evaluations/`, or `scripts/`.

## Summary

| ID | Artifact that can prove the SPEC wording today | Honest status |
|---|---|---|
| POC-001 | Live four-transport budget path in `scripts/demo --assert` | Evidence exists; not signed off until that gate produces `target/wp200/demo-report.json` |
| POC-002 | Budget fixtures and `./scripts/budget-safety-demo --assert`, not `command_concurrency.rs` | Real 80/80/100 evidence exists; candidate manifest cites the wrong test |
| POC-003 | `generated_histories_match_runtime_and_reference_model` | In-memory runtime vs reference model only; not durable committed state |
| POC-004 | `full_recovery_matrix` response-loss cases plus SDK `replayed` mapping | Strong public-gRPC replay evidence; wire field is `status=Replayed`, not JSON `replayed=true` |
| POC-005 | Recovery graph assertions and safety-report replay counts | Strong same-key uniqueness evidence on the recovery and safety paths |
| POC-006 | Projection prefix/recovery tests and composition `required_sequence` | Frontier monotonicity exists; `after_sequence=N` as a public query argument is not the projection test's API |
| POC-007 | `contract_deploy_changes_active_session_catalog_and_emits_list_notifications` | Catalog change and `notifications/tools/list_changed` exist in MCP conformance; live demo does not wait for the notification |
| POC-008 | Adapter architecture tests plus `authorization_non_bypass.rs` | Shared-service/auth architecture evidence exists; not a single all-API provenance gate |
| POC-009 | WP-190 inventory of 41, process-kill matrices execute 18 | Incomplete relative to “every defined failpoint”; zero `ProductionGap` rows, but 23 cases are not those two binaries |
| POC-010 | `scripts/demo`, `scripts/ci-all`, `scripts/release-poc`, `adr/` | Pieces exist; candidate report is unexecuted; `release-poc --verify` embeds `./scripts/ci-all` |

## POC-001

**SPEC:** The annual budget example compiles, deploys, and executes from the
CLI, gRPC, Rust SDK, and MCP.

**Evidence that exists**

- Contract: `contracts/examples/budget.riff`.
- `scripts/demo --assert` (`run_public_budget_case`): live `riffdbd`, CLI
  `command execute CreateBudget` / `AllocateBudget` with
  `release/config/budget-create-v1.json` and
  `release/config/budget-allocate-v1.json`; Rust SDK over public gRPC via
  `examples/budget-comparison/riffdb-grpc` (`riffdb-budget-public`) for
  `sequential`, `contention`, and `same_key_replay`; MCP stdio through
  `riffdb-mcp` for create, allocate, provenance, and projection.
- Hosted composition: `tests/server_composition/server_composition.rs`
  test `real_process_hosts_policy_filtered_mcp_and_stops_on_sigterm`
  (`cargo test -p riffdb-server --test server_composition`).
- MCP transports: `tests/mcp/mcp_conformance.rs` test
  `hosted_http_matches_stdio_and_rejects_authentication_bypasses`
  (`cargo test -p riffdb-api-mcp --features stdio,streamable-http --test mcp_conformance`).

**Missing**

- No passing `target/wp200/demo-report.json` on this revision. The candidate
  manifest still says `requires_execution`.
- Direct raw gRPC without CLI or the generated Rust SDK is not a demo case.
  gRPC is exercised as the transport under CLI and the SDK, which matches
  how those products are used, but it is not a fourth independent client.
- No `req: POC-001` tag.

## POC-002

**SPEC:** With 100 units remaining, two concurrent 80-unit allocations
cannot both commit.

**Evidence that exists**

- Frozen 80/80/100 observation:
  `examples/budget-comparison/fixtures/contention-observation-v1.json`
  (`approved_amount` `100.00`, two requested `80.00`, one `Allocated` and
  one `InsufficientBudget`, final allocated `80.00`).
- Same numbers in
  `examples/budget-comparison/fixtures/safety/report-v1.json` scenario
  `lost_update_without_lock` and
  `examples/budget-comparison/fixtures/workload-v1.json`.
- Live checked report: `./scripts/budget-safety-demo --assert`, compared
  byte-for-byte to
  `examples/budget-comparison/fixtures/safety/report-v1.jsonl` by
  `scripts/demo --assert`.
- Demo live case `contention` through `riffdb-budget-public`.

**Missing / mismatch**

- `release/evidence/poc-requirements-v1.json` POC-002 evidence[0] names
  `tests/command_semantics/command_concurrency.rs` and asserts “two
  concurrent 80.00 allocations against 100.00 cannot both allocate”.
  That file has no `80.00`, no `100.00`, and no `AllocateBudget`. Its
  budget helper uses `12_500` minor units
  (`tests/command_semantics/support.rs`). It is coordinator concurrency
  evidence, not the 80/80/100 criterion.
- The real 80/80/100 artifacts are the budget-comparison fixtures and
  `./scripts/budget-safety-demo --assert` listed as evidence[1] in the
  same POC-002 block.

## POC-003

**SPEC:** Every committed state satisfies all supported invariants.
Verification: property tests over generated command histories.

**Evidence that exists**

- `crates/riffdb-testkit/src/histories/mod.rs` test
  `generated_histories_match_runtime_and_reference_model`
  (`cargo test -p riffdb-testkit generated_histories`): 216 three-step
  cartesian histories of Create/Allocate on the LegalSpend budget
  contract. Each step calls `riffdb_runtime::execute_command` twice,
  asserts determinism, and matches an in-process `ReferenceModel`.
- `examples/budget-comparison/tests/public_comparison.rs`: public gRPC
  runner observations vs the shared budget oracle (process-level, not a
  generated-history property).

**Missing**

- The history test never opens storage, never assigns a commit sequence,
  and never re-reads a durable entity. It evaluates the deterministic
  runtime against a reference model. That is not “every committed state”.
- It is exhaustive over a 6³ budget-only operation set, not a property
  test over generated command histories of the whole language.
- No invariant checker over an arbitrary committed store snapshot.

## POC-004

**SPEC:** A post-commit connection loss followed by retry returns the
original outcome with `replayed=true`.

**Evidence that exists**

- `tests/recovery/full_recovery_matrix.rs` functions
  `run_response_loss_case`, `assert_replayed_allocate`: cases
  `command.public-response.kill-riffdbd` and
  `command.public-response.partial-frame`. After loss, `GetOutcome` and
  `execute_with_retry` both require
  `CompletionStatus::Replayed`, commit sequence `2`, and unchanged
  allocated amounts. Owned by
  `cargo test -p riffdb-testkit-server --test full_recovery_matrix -- --ignored`
  (see package mismatch below).
- `crates/riffdb-client-rust/src/application.rs` maps
  `CompletionStatus::Replayed` to `TypedCommandResult.replayed: bool`.
- In-process coordinator: `tests/command_semantics/lost_response_replay.rs`
  `committed_unknown_response_replays_exactly_after_reopen` asserts
  `CommittedOutcomeDisposition::Replay`.
- Safety fixture scenario `duplicate_retry_after_discarded_response` in
  `examples/budget-comparison/fixtures/safety/report-v1.json`
  (`replay_completion: "Replayed"`).

**Missing**

- No test asserts a JSON field literally named `replayed=true`. The
  public protobuf field is `status = Replayed`. The boolean lives on the
  Rust SDK result type.
- Candidate manifest command
  `cargo test -p riffdb-testkit --test full_recovery_matrix -- --ignored`
  is the wrong package (see Cross-cutting).

## POC-005

**SPEC:** One logical command creates at most one mutation set and one
durable event set for a given idempotency key.

**Evidence that exists**

- `tests/recovery/full_recovery_matrix.rs` `assert_durable_graph` and the
  post-replay entity-version check (`entity.entity_version != 2` fails as
  “zero or multiple authoritative mutations”).
- Safety report `duplicate_retry_after_discarded_response`:
  `matching_allocation_commit_count: 1`,
  `allocation_commit_event_count: 1`. Demo maps those to
  `duplicate_mutation_count: 0` and `duplicate_event_count: 0`.
- `tests/command_semantics/command_concurrency.rs`
  `equal_key_commands_commit_once_and_replay_exactly`.

**Missing**

- Demo’s duplicate counts are derived from the checked safety JSON, not
  from a live storage dump in the demo process.
- No `req: POC-005` tag.

## POC-006

**SPEC:** The projection frontier is monotonic and `after_sequence=N`
never returns a result missing commit N.

**Evidence that exists**

- `tests/projection/projection_prefix_and_recovery.rs`
  `prefix_frontier_duplicate_rebuild_and_recovery_are_generation_safe`
  asserts `FrontierPosition::AppliedThrough(sequence(2))` after apply and
  after rebuild; `grouped_sum_overflow_fails_before_state_or_frontier_changes`
  refuses a frontier move on overflow.
- `after_sequence_wait_returns_to_the_service_on_notification_or_cancellation`
  in the same file exercises
  `ProjectionReadSource::observe_after_sequence`.
- `tests/server_composition/server_composition.rs` uses
  `required_sequence: Some(2)` / MCP `required_sequence: "2"` and
  `assert_applied_through`.
- Demo MCP `riffdb_projection_query` with `required_sequence: "2"` and
  asserts `frontier.applied_through == "2"`.

**Missing**

- The public protobuf field is `after_sequence` on commit scan
  (`proto/riffdb/v1/commit.proto`). Projection waits use
  `required_sequence` / `observe_after_sequence`. There is no test whose
  name or assertion is exactly “`after_sequence=N` never returns a result
  missing commit N” on the projection query API.
- The 80/80/100 concurrent-allocation claim is **not** a POC-006
  artifact. The candidate manifest attaches that assertion to POC-002
  (see POC-002). POC-006’s own evidence rows name projection and
  composition tests, and those files do exist.

## POC-007

**SPEC:** Contract deployment changes the MCP command tool catalog and
emits a list-changed notification when supported.

**Evidence that exists**

- `tests/mcp/mcp_conformance.rs`
  `contract_deploy_changes_active_session_catalog_and_emits_list_notifications`:
  after `riffdb_contract_deploy`, asserts methods
  `notifications/tools/list_changed` and
  `notifications/resources/list_changed`, then that `tools/list` contains
  the deployed command tool and not the stale one.
- Implementation: `crates/riffdb-api-mcp/src/observer.rs`,
  `hosted_observer.rs`, `stdio_transport.rs`.
- Demo MCP `tools/list` after deploy sees
  `riffdb_cmd_legalspend_createbudget` and
  `riffdb_cmd_legalspend_allocatebudget`; a restricted principal sees
  zero `riffdb_cmd_*` tools.

**Missing**

- `scripts/demo` MCP case does not subscribe for or assert
  `notifications/tools/list_changed`. Catalog contents are checked;
  the notification is not.
- Conformance deploy uses a stub contract
  (`contract LegalSpend version 3 {}`), not the annual budget example.

## POC-008

**SPEC:** All mutation paths pass through shared authorization, command
execution, and provenance code regardless of API.

**Evidence that exists**

- `tests/authorization/authorization_non_bypass.rs`
  `exact_command_scope_and_field_obligations_cannot_be_bypassed`,
  `real_authentication_state_is_rechecked_at_every_policy_safe_point`
  (`cargo test -p riffdb-policy --test authorization_non_bypass`).
- `crates/riffdb-api-grpc/tests/architecture.rs`
  `grpc_adapter_has_no_lower_semantic_authority_dependency`,
  `bounded_batch_ingress_authenticates_once_without_bypassing_the_application_service`.
- `crates/riffdb-api-mcp/tests/hosted_architecture.rs`
  `hosted_authentication_has_one_owner_and_credential_never_enters_session_state`.
- `crates/riffdb-cli/tests/architecture.rs`
  `source_has_no_internal_database_or_unchecked_transport_path`.
- `examples/budget-comparison/tests/safety_architecture.rs`: comparison
  and unsafe PostgreSQL controls stay outside production/benchmark paths.
  This is isolation evidence, not a shared mutation-path proof.

**Missing**

- No single architecture test that names CLI, gRPC, MCP, and SDK and
  asserts they call the same authorize → execute → provenance functions.
- Candidate manifest POC-008 evidence[1] (`safety_architecture.rs`) does
  not prove shared authorization/execution/provenance. It proves the
  comparison harness is non-production.

## POC-009

**SPEC:** The server recovers from every defined failpoint without torn
committed state. Verification: process-kill recovery matrix.

**Evidence that exists**

- Closed inventory: `crates/riffdb-testkit/src/failpoint.rs`
  `RECOVERY_SCENARIOS` (41 rows). `WP190_PRODUCTION_GAP_IDS` is empty.
- Process-kill / dedicated-child cases actually executed by the two WP-190
  ignored binaries: `WP190_EXECUTED_CASE_IDS` (18 ids), rendered into
  `tests/recovery/fixtures/wp190-report-v1.json` with
  `"inventory_cases": 41, "executed_cases": 18, "production_gap_cases": []`.
- Those 18 are the two public-response cases, CLI bootstrap retention,
  maintenance daemon, backup/receipt/restore adapter failpoints.
- Consumption pin:
  `release/evidence/wp190-recovery-consumption-v1.json`.
- Additional owner-package and other-binary rows in the same inventory
  point at `storage_recovery_matrix`, `contract_deploy_recovery`,
  `outbox_crash_and_duplicate`, `projection_prefix_and_recovery`,
  `offline_maintenance_recovery`, and replication follower tests.

**Missing**

- SPEC says every defined failpoint via a process-kill recovery matrix.
  The matrix binaries execute 18 of 41 inventory rows. The other 23 are
  `OwnerPackage` crate tests or other packages’ crash tests, not the two
  WP-190 executables. That is a coverage-class split, not a hidden pass.
- `maintenance.public.backup-restore-rewind`,
  `replication.applier.crash`, `replication.bootstrap.crash`,
  `replication.stream.kill-riffdbd`,
  `startup.repeated-complete-validation`, and the storage/migration
  abort rows are inventory entries whose proof is a different command
  than `full_recovery_matrix` / `offline_maintenance_matrix`.
- Acceptance commands in both
  `tests/recovery/fixtures/wp190-report-v1.json` and
  `release/evidence/wp190-recovery-consumption-v1.json` name
  `-p riffdb-testkit`. The `[[test]]` targets live in
  `crates/riffdb-testkit-server/Cargo.toml`. `scripts/demo` already uses
  `-p riffdb-testkit-server`. The checked report’s commands would not
  run the tests they describe.

## POC-010

**SPEC:** The repository contains reproducible builds, CI, security
checks, documented ADRs, and a one-command local demo.

**Evidence that exists**

- One-command demo script: `scripts/demo --assert` (requires a clean
  tree, Docker or `RIFFDB_BUDGET_POSTGRES_URL`, and a long test battery).
- CI entry: `scripts/ci-all`.
- Release assembly: `scripts/release-poc --verify` (reproducible
  `cmp` of two release builds, SBOM, checksums, systemd smoke). That
  script also runs `./scripts/demo --assert` and `./scripts/ci-all`.
- ADRs: `adr/` with accepted records listed by WP-200.
- Security/policy: `cargo deny` / `cargo audit` via `scripts/release-poc`
  tool-version checks and `scripts/ci-all`.
- Engine comparison: `benchmarks/storage-fjall` (readable this round;
  not re-run here).
- Benchmark status pointer: `benchmarks/reports/status-v1.json`.

**Missing**

- Candidate manifest POC-010 is `requires_execution`; demo report would
  set it to `requires_release_verification` until `release-poc --verify`
  finishes.
- No architecture-review “next-stage decision” record, which is WP-200’s
  exit_gate alongside POC sign-off.
- `scripts/release-poc --verify` cannot be a memory-light gate: it
  embeds `./scripts/ci-all`.

## Cross-cutting mismatches in `poc-requirements-v1.json`

Do not treat these as passing because the JSON says they are evidence.

| Claim in the candidate manifest | Artifact reality |
|---|---|
| POC-002 / `command_concurrency.rs` “two concurrent 80.00 allocations against 100.00” | File does not contain that scenario. Real 80/80/100 is `examples/budget-comparison/fixtures/{contention-observation-v1.json,safety/report-v1.json,workload-v1.json}` plus `./scripts/budget-safety-demo --assert`. |
| POC-003 / `cargo test -p riffdb-testkit generated_histories` as committed-state proof | `generated_histories_match_runtime_and_reference_model` is in-memory `execute_command` vs `ReferenceModel`. |
| POC-004 and POC-009 / `cargo test -p riffdb-testkit --test full_recovery_matrix` | Tests are registered on `riffdb-testkit-server`. Same wrong package in `tests/recovery/fixtures/wp190-report-v1.json` and `release/evidence/wp190-recovery-consumption-v1.json`. |
| POC-006 / composition “gRPC and MCP read-after-sequence queries include the required commit” | Composition uses `required_sequence`, not protobuf `after_sequence`. |
| POC-008 / `safety_architecture.rs` as shared mutation-path proof | That test isolates comparison code from production; it does not prove shared auth/execute/provenance. |
| POC-009 / “complete checked failpoint inventory has no unresolved production gap” | Inventory is 41; WP-190 binaries execute 18. `production_gap_cases` is empty by classification, not because 41 process-kill cases ran. |
| Whole file `attestation: candidate_not_executed` | No requirement in this file is `verified` on disk. |

## What the repaired gate exposed (2026-09-18)

`scripts/demo --assert` had been unrunnable behind three stale nested lockfiles
(`examples/budget-comparison`, `benchmarks/command-growth`,
`benchmarks/storage-fjall`). With those repaired the gate runs to completion for
the first time and fails on a real assertion, not on tooling:

`benchmarks/storage-fjall/tests/conformance.rs`,
`accepted_registry_is_exactly_ninety_four_readable_and_seventy_one_writable`,
freezes the durable record-schema registry at 94 readable and 71 writable.
`crates/riffdb-proto/src/durable.rs` now declares 111 readable and 88 writable,
so the freeze is 17 behind on each side.

The growth is recent and ongoing: the registry last grew in WP-748's replication
administration receipt codec and bounded follower audit V3 codec, and WP-748 is
still adding codecs. The number will keep moving while that package is open.

This audit does not change that assertion. Updating a frozen durable-format
conformance count asserts that every added schema is legitimately accepted, which
is a durable-format governance decision owned by whoever owns the registry
growth, not by a lockfile repair. It is recorded here as the next blocker on
POC-010 and on the demo gate.

## What remaining WP-200 work this sizes

1. Make `./scripts/demo --assert` produce `target/wp200/demo-report.json`
   on a clean revision (nested lockfile pin is a gate, not evidence).
2. Correct or replace the candidate-manifest rows that name the wrong
   test, package, or claim (maintainer act; this audit does not edit it).
3. Decide whether POC-003 needs durable committed-state histories, or
   whether the in-memory 216-history test is the accepted reading.
4. Decide whether POC-009 is the 18 executed WP-190 rows plus named
   owner-package commands, or a true 41-row process-kill matrix.
5. Add `req: POC-001` … `POC-010` tags on the tests that will close the
   package.
6. Run `./scripts/release-poc --verify` (includes `./scripts/ci-all`) and
   record the architecture next-stage decision. Package closure stays
   with the maintainer.
