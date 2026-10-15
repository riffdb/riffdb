---
adr: "0311"
title: Capability Inventory, Decision-Arm Workflows, and Offline Storage Compaction
status: accepted
tier: guarantee
date: 2026-10-15
accepted: 2026-10-15
acceptance: 'maintainer, in session, 2026-10-15: selected "Capability inventory & inspection", "Branch-local workflow transitions & lease instructions inside decide arms", and "Offline storage compaction (riffdb storage compact)"'
requires: [ADR-0052, ADR-0061, ADR-0071, ADR-0092, ADR-0118, ADR-0170, ADR-0256]
amends: [ADR-0118, ADR-0170]
supersedes: []
requirements: [SEC-001, SEC-002, SEC-003, CMP-001, CMP-013, WF-001, WF-002, WF-007, TXN-001, STO-001, STO-010, STO-012, DX-014]
packages: [WP-862, WP-863, WP-864]
obligations: []
review_triggers:
  - Capability inventory or inspection output exposes raw bearer tokens, secret hashes, or unredacted credential files.
  - Decision-arm workflow transitions or leases bypass optimistic revision fencing, state preconditions, lease holder/epoch checks, or commit-time zero-mutation rejection validation.
  - Offline storage compaction runs against an unrecovered or open database without checkpointing and verifying retained V3 history.
---
# ADR-0311: Capability Inventory, Decision-Arm Workflows, and Offline Storage Compaction

## Context

Three explicit entries in `docs/known-limitations.md` restrict day-2 security administration, contract command expressiveness, and storage capacity management:

1. **No operator-facing capability inventory or inspection command**: `riffdb capability` provides `bootstrap`, `create`, and `revoke`, and `crates/riffdb-storage-api/src/capability.rs` already defines `CapabilityInventoryReader::scan_capabilities` and `CapabilityReader::read_capability` implemented in `crates/riffdb-storage-redb/src/administration.rs`. However, the CLI exposes neither `riffdb capability list` nor `riffdb capability show`, forcing operators to track issued capability IDs manually in external spreadsheets.
2. **Workflow `transition` and `lease` instructions rejected inside `decide` arms**: `crates/riffdb-contract-syntax`, `crates/riffdb-contract-ir`, and `crates/riffdb-contract-compiler/src/command_lowering.rs` already parse, encode, decode, and lower `Instruction::WorkflowTransition` and `Instruction::WorkflowLease` inside `CommandDecisionActionV1::Apply`. However, `crates/riffdb-contract-compiler/src/command_analysis.rs` unconditionally rejects them in `analyze_decision_action`, `crates/riffdb-contract-compiler/src/hir.rs` deduplicates transitions/leases globally across mutually exclusive arms and checks workflow revision/lease inputs only on top-level effects, `crates/riffdb-runtime/src/lib.rs` ignores them in `execute_ordinary_decision_effects`, and `crates/riffdb-commit/src/command_validation.rs` checks only top-level instructions when validating zero-mutation workflow/lease rejection outcomes.
3. **No offline storage compaction after retention pruning**: Retention pruning frees pages inside the redb file for internal reuse, but `riffdb storage` exposes no `compact` command to compact a closed database file and lower its on-disk high-water mark after large retention pruning runs.

## Decision

1. **Operator Capability Inventory & Inspection (`WP-862`)**:
   - Expose `RedbOfflineCapabilityInventory` in `crates/riffdb-storage-redb/src/administration.rs` to inspect `CAPABILITIES`, `META_DATABASE_ID`, and `META_CAPABILITY_BOOTSTRAP` on a closed database file using bounded cursor scans (`StorageScanLimit`), returning `StoredCapabilityRecordV1` metadata (`capability_id`, `principal_id`, `status`, `issued_at_commit_seq`, `revoked_at_commit_seq`, `expires_at_unix_millis`, `allowed_Scopes`, `allowed_commands`, `allowed_queries`, `allowed_views`, `allowed_streams`, `role_bindings`, and `is_bootstrap`) while never exposing `token_hash` or secret material.
   - Maintain an operator-local non-secret inventory ledger file (via `--inventory-file <PATH>` or alongside `<credential_output>.inventory.json`) during `riffdb capability bootstrap`, `riffdb capability create`, `riffdb capability revoke`, `riffdb role bind`, and `riffdb role revoke`.
   - Add `riffdb capability list` (`CommandIdentity::CapabilityList`) and `riffdb capability show` (`CommandIdentity::CapabilityShow`) in `crates/riffdb-cli/src/cli.rs`, supporting both `--database-path <PATH>` (authoritative redb inspection) and `--inventory-file <PATH>` (operator ledger inspection), plus `--status`, `--principal`, `--after`, and `--limit` filters.
2. **Branch-Local Workflow Transitions & Lease Instructions Inside `decide` Arms (`WP-863`)**:
   - In `crates/riffdb-contract-compiler/src/hir.rs`, scope `transitioned_bindings` and `leased_bindings` deduplication by `(decision_action, binding.id)` so mutually exclusive `when`/`else` arms may each declare one workflow transition and one lease instruction per binding; permit `BindingMode::ObserveOrInitialize` alongside `BindingMode::Mutate` and `BindingMode::InitOrMutate`; and verify workflow revision/lease inputs across both top-level `effects` and `decisions` arm effects.
   - In `crates/riffdb-contract-compiler/src/command_analysis.rs`, validate `HirEffect::WorkflowTransition` and `HirEffect::WorkflowLease` inside `analyze_decision_action` using the same field-write disjointness, binding mode, influential-root, and rejection-outcome rules as top-level effects.
   - In `crates/riffdb-runtime/src/lib.rs`, execute `Instruction::WorkflowTransition` and `Instruction::WorkflowLease` inside `execute_ordinary_decision_effects` (and `execute_collection_decision_effects`), enforcing expected revision, allowed source state, and lease holder/epoch preconditions on the selected arm and returning `DecisionEffectOutcome::Rejected(outcome)` with zero mutations when a branch-local transition or lease precondition fails.
   - In `crates/riffdb-commit/src/command_validation.rs`, inspect both `plan.instructions()` and `plan.decisions()` arm instructions when validating declared zero-mutation workflow transition and lease rejection outcomes.
3. **Offline Storage Compaction (`WP-864`)**:
   - Expose `RedbOfflineCompaction` in `crates/riffdb-storage-redb/src/lib.rs` and `riffdb storage compact --database-path <PATH>` (`CommandIdentity::StorageCompact`) in `crates/riffdb-cli/src/cli.rs`.
   - `RedbOfflineCompaction` runs format preflight, opens `RedbStore` to replay/checkpoint any journal suffix and validate retained V3 history via `prepare_offline_retention`, closes `RedbStore`, invokes `redb::Database::compact()` on the authoritative database file (and `<database_path>.derived` when present) followed by file sync, reopens `RedbStore` to re-verify post-compaction state and record a clean close marker, and reports `database_id`, `bytes_before`, `bytes_after`, `reclaimed_bytes`, and `compacted`.
   - Remove the three resolved entries from `docs/known-limitations.md` and update the corresponding handbook pages.

## Options considered

1. **Keep workflow transitions/leases top-level only and require separate commands per branch:** rejected; forces applications to split single atomic state-machine decisions across multiple round-trips or duplicate precondition logic.
2. **Online compaction during live commits:** rejected; `redb::Database::compact()` requires exclusive mutable access without concurrent read/write transactions, making explicit offline compaction the safe operational boundary.
3. **Bounded offline redb inspection + local ledger for capabilities, branch-local workflow/lease execution in `decide` arms, and verified offline storage compaction:** selected; eliminates all three limitations without weakening any concurrency, durability, or security guarantee.

## Consequences

- Operators can audit and inspect all issued and revoked capabilities directly from the database or local inventory ledger without tracking capability IDs by hand.
- Contract authors can combine `decide { when ... => { transition ...; lease ...; } else => ... }` in a single atomic command while preserving existing IR encoding and plan hashes for contracts that do not use decision-arm transitions/leases.
- Operators can reclaim physical disk space after retention pruning via `riffdb storage compact`.

## Standing design tests

- **Interface safety:** Capability inspection never reads, stores, or prints raw bearer tokens or secret hashes. Decision-arm workflow transitions and leases enforce the exact same revision, state, and lease fences and commit-time validation as top-level instructions. Offline compaction requires a closed database and verifies retained V3 history before and after compacting.
- **Scale:** Capability listing uses `StorageScanLimit` pagination, decision-arm workflow/lease evaluation adds $O(1)$ per-arm checks within existing command fuel bounds, and offline compaction streams through `redb`'s bounded page allocator.

## Checks

- `capability_list_and_show_cli_inspect_authoritative_and_ledger_records_without_secrets` in `crates/riffdb-cli`.
- `decision_arm_workflow_transition_and_lease_instructions_compile_execute_and_commit` in `crates/riffdb-runtime`.
- `offline_storage_compact_reclaims_freed_pages_and_preserves_authoritative_state` in `crates/riffdb-storage-redb`.
