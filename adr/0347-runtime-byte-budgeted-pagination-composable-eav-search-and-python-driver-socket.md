---
adr: "0347"
title: Runtime byte-budgeted cursor pagination, composable EAV candidate search, and Rust-owned Python driver socket transport
status: accepted
tier: surface
date: 2026-11-11
accepted: 2026-11-11
acceptance: 'maintainer, in session, 2026-11-11: "yes"'
requires: ["0051", "0074", "0108", "0174", "0200", "0307", "0344"]
amends: ["0074", "0200", "0307", "0344"]
supersedes: []
requirements: ["QRY-001", "QRY-002", "QRY-007", "RQL-002", "RQL-003", "DRV-001", "DRV-005", "DRV-012", "DX-004", "HBK-003"]
packages: ["WP-926"]
obligations:
  - id: OBL-0347-1
    package: WP-926
    description: "Cursor-paginated single-result Many steps with wide fields (> 64 KiB) split pages at runtime using the exact encoded-snapshot byte model while non-cursor queries retain static N * row_bytes enforcement"
    proof: runtime_byte_budgeted_pagination_bounds_wide_rows_and_resumes_cursor
  - id: OBL-0347-2
    package: WP-926
    description: "Composable optional candidate intersections, set-in k-way merges, and optional point-lookup ORDER BY ... NULLS LAST execute within static scan and hydration bounds"
    proof: composable_eav_predicates_and_joined_key_order_by_nulls_last_execute
  - id: OBL-0347-3
    package: WP-926
    description: "First-party Rust Python driver socket transport enforces 8 MiB FrameCodec framing, verifies handshake ACK identity and role, bounds socket I/O timeouts, and classifies post-send command disconnects/timeouts as OutcomeUnknown"
    proof: socket_command_post_send_disconnect_and_timeout_report_outcome_unknown
  - id: OBL-0347-4
    package: WP-926
    description: "CLI capability bootstrap --force validates bootstrap inputs and rejects path/inode aliasing before removing existing regular output files, and deploy --environment selects only locked manifest roles"
    proof: bootstrap_force_validates_inputs_and_rejects_aliasing_before_removing_existing_files
review_triggers:
  - "Static encoded-result byte relief is granted to a query shape that does not pass through the runtime cursor page byte limiter."
  - "Python runtime owns socket framing, handshake validation, or error classification outside crates/riffdb-client-python-native."
  - "capability bootstrap --force removes an existing file before validating bootstrap inputs or checking path/inode aliasing."
---
# ADR-0347: Runtime byte-budgeted cursor pagination, composable EAV candidate search, and Rust-owned Python driver socket transport

## Context

Downstream adapter integration across MLflow, LangGraph, OpenFGA, and Better Auth surfaced four operational and query-planning gaps:

1. **Wide-field cursor pagination (`bytes<1048576>`, `string<1048576>`):** Charging `Limit<MAX> * max_field_bytes` statically rejected `Limit<100>` cursor-paginated queries over 1 MiB blob/text columns even when almost all rows are small and pages can be split dynamically at runtime with a continuation cursor.
2. **Composable EAV search and joined-metric sorting:** Multi-filter EAV search (`candidates` blocks with `when present(...)` predicates, `in $set` filters, operational comparisons, and `where` clauses) combined with post-hydration `where` predicates and `order by joined_metric.value asc|desc nulls last` required manual N+1 round-trips in application code.
3. **Python Unix domain socket driver transport:** Python applications configured with `RIFFDB_DRIVER_SOCKET` or `unix://` endpoints needed local `riffdb-driverd` socket support inside the first-party Rust PyO3 extension (`crates/riffdb-client-python-native`) with `FrameCodec` 8 MiB framing, verified handshake ACK identity and `role` selection, bounded I/O timeouts, and `OutcomeUnknown` classification on post-send command disconnects.
4. **CLI bootstrap `--force`, role `--environment`, and lock-drift diagnostics:** `capability bootstrap --force` needed to validate inputs and reject path/inode aliasing before overwriting target files; `application deploy --environment` needed to select a locked manifest role for that environment without mutating role grant identity; and lock-drift diagnostics needed to inspect the canonical `"modules"` and `"artifacts"` keys.

## Decision

1. **Runtime byte-budgeted cursor pagination (`crates/riffdb-query-compiler`, `crates/riffdb-query-executor`):**
   - Static single-unit byte sizing (`single_unit_bytes`) applies only when a query has no aggregates, no nested `for each` expansion, a single top-level result, and a cursor-paginated (`cursor_parameter().is_some()`) `Many` access step (`Index`, `PartitionSetIndex`, or `CandidateRootHydration`) projecting a wide field (`> 65,536` bytes). Non-cursor queries, aggregate queries, and `ExpansionIndex` steps remain statically bounded by `rows * worst_case_row_bytes <= 4 MiB`.
   - At runtime, `step_page_byte_budget_v1` and `estimated_*_row_result_bytes_v1` use the exact `encoded_snapshot_bytes` fuel model (`1,024` base + per-result overhead, `64 + entity.len()` per row, `160 + field_name.len() + canonical_value_encoded_len(v)` per field). Once at least one row is collected, if the next row would exceed `MAX_ENCODED_RESULT_BYTES_V1` (4 MiB), the page terminates early and emits a continuation cursor positioned immediately before that row.
2. **Composable optional `candidates`, post-hydration `where`, and joined `order by ... nulls last` (`crates/riffdb-riffql-syntax`, `crates/riffdb-query-compiler`, `crates/riffdb-query-executor`):**
   - `candidates` sources support `when present($param)` guards, `in $set` k-way merge sources, operational comparisons/prefixes, and `where` filters; omitted optional branches evaluate to the intersection identity (`All`) with fallback to a declared base source when all optional branches are omitted.
   - `hydrate candidates` steps support `where` predicates, dependent `= get maybe` point-lookup steps (including `when present(...)` and nullable foreign keys), and `order by binding.field asc|desc nulls last` sorting before `take` and cursor emission.
3. **First-party Rust Python driver socket transport (`crates/riffdb-driver-host`, `crates/riffdb-client-python-native`, `clients/python/runtime`):**
   - `DriverSocketState` in `crates/riffdb-client-python-native` implements the Unix socket transport using `tokio::net::UnixStream` and `FrameCodec` (`MAX_DRIVER_FRAME_BYTES` = 8 MiB) with bounded connect, handshake, and invocation timeouts.
   - Handshake sessions are keyed by `(contract_lineage, contract_version, contract_bundle_hash, database, role)`, strictly validate the `DriverResponse::Handshake` ACK and per-request `request_id`, and classify any post-send I/O failure, EOF, or timeout during `execute_command` as `OutcomeUnknown`.
4. **CLI bootstrap `--force` safety, locked `--environment` role selection, and lock-drift reporting (`crates/riffdb-cli`):**
   - `bootstrap_material` rejects path/inode aliasing between primary and bearer files, loads and validates `--bootstrap-file` / `--bootstrap-stdin` before removing any target under `--force`, and removes only existing regular non-symlink target files immediately before writing.
   - `application deploy --environment <ENV>` selects a role declared with `environment == <ENV>` in `riffdb.application.lock.json` without mutating compiled role identity.

## Options considered

- **Implementing the Python driver socket protocol in pure Python:** Rejected by `AGENTS.md` Boundary 1 and ADR-0074; protocol framing, handshake verification, timeout/cancellation ownership, and `OutcomeUnknown` classification belong in first-party Rust (`crates/riffdb-client-python-native`).
- **Granting static byte-budget relief to non-cursor `Many` steps:** Rejected because only cursor-paginated steps can emit a continuation cursor when stopping early at the 4 MiB page boundary.

## Consequences

- Cursor-paginated queries over `bytes<1048576>` / `string<1048576>` and multi-attribute EAV search with joined sorting execute safely within the 4 MiB response ceiling.
- Python SDK `connect_driver_socket`, `connect_uri("unix://...")`, and `connect_from_env` share the Rust protocol core and fail closed with `OutcomeUnknown` on post-send command disconnects.

## Standing design tests

- **Interface safety:** Non-cursor queries cannot opt out of static 4 MiB bounds; post-send command socket failures always raise `OutcomeUnknown`; `bootstrap --force` never deletes an aliased or pre-validation file; `--environment` never synthesizes an unlocked role grant.
- **Scale:** Runtime byte-budgeted pages never exceed `MAX_ENCODED_RESULT_BYTES_V1` (4 MiB), and socket frames never exceed `MAX_DRIVER_FRAME_BYTES` (8 MiB).

## Checks

- Proof tests `runtime_byte_budgeted_pagination_bounds_wide_rows_and_resumes_cursor`, `composable_eav_predicates_and_joined_key_order_by_nulls_last_execute`, `socket_command_post_send_disconnect_and_timeout_report_outcome_unknown`, and `bootstrap_force_validates_inputs_and_rejects_aliasing_before_removing_existing_files`.
- `./scripts/check-adr-form`, `./scripts/check-adr-obligations`, `./scripts/check-generated`, and `./scripts/handbook check`.
