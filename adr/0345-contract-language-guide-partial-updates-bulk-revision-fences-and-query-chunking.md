---
adr: "0345"
title: Contract language guide, partial update coalesce expressions, bulk loop revision fences, and SDK query chunking
status: accepted
tier: guarantee
date: 2026-11-08
accepted: 2026-11-08
acceptance: 'maintainer, in session, 2026-11-08: "yes"'
requires: ["0062", "0120", "0121", "0137", "0253", "0303", "0304"]
amends: ["0062", "0120", "0253", "0304"]
supersedes: []
requirements: ["CMP-001", "DX-012", "DX-050", "HBK-003", "HBK-006", "QRY-001", "QRY-002"]
packages: ["WP-924"]
obligations:
  - id: OBL-0345-1
    package: WP-924
    description: "Contract language and invariant evaluator support coalesce(a, b) and a ?? b over optional<T> inputs and generated SDK command inputs allow omitting optional<T> fields for partial updates"
    proof: partial_update_coalesce_preserves_unspecified_fields_under_revision_fence
  - id: OBL-0345-2
    package: WP-924
    description: "Bulk command for-loops automatically recognize dominating iteration-local aggregate-root revision equality checks and +1 increments as CallerVersionFence write evidence without redundant checked_by annotations"
    proof: bulk_loop_caller_version_fence_guards_element_mutations_without_checked_by
  - id: OBL-0345-3
    package: WP-924
    description: "Generated TypeScript, Go, and Python SDK clients expose <query>All helpers and bound constants that chunk Set<T> parameters and paginate cursors under a bounded page limit pinned to applicationHead"
    proof: generated_sdk_query_all_chunks_bounded_sets_and_paginates_cursors
review_triggers:
  - "Coalesce evaluation accepts mismatched scalar bounds or coerces non-null falsy values (false, 0, empty string) instead of checking nullability."
  - "Bulk loop CallerVersionFence recognition admits a write that occurs before the revision requirement or on a binding from a different loop scope."
  - "Generated SDK <query>All helpers perform unbounded pagination or omit readAfterCommit pinning across pages or chunks."
---
# ADR-0345: Contract language guide, partial update coalesce expressions, bulk loop revision fences, and SDK query chunking

## Context

Building the Todo application and reviewing contract/query authoring surfaced four developer-experience gaps:

1. **No readable contract and query pattern guide:** `docs/contracts/README.md` linked directly to the 1,392-line generated LALRPOP grammar (`docs/contracts/LANGUAGE.md`) and opened with advanced `init_or_mutate` / `decide` constructs instead of everyday patterns (entity declaration, `optional<T>` fields, aggregate routing, create, update with a revision check, partial update, delete, bulk command, and matching `.riffq` queries).
2. **No partial update expression in `.riff` commands:** Updating a subset of fields on an entity (such as toggling `done` or renaming `title` without resending every attribute) required defining a separate command per field combination (`SetTodoDone` alongside `UpdateTodo`), followed by re-locking, regenerating SDKs, and redeploying.
3. **Inconsistent `RDB-C048` blind-write checks in `bulk command` loops:** In a single command, `mutate Root(...) as todo`, `require rev: todo.revision == expected_revision else Stale {}`, and `set todo.revision = todo.revision + 1` automatically satisfy `CallerVersionFence` for every `set todo.<field> = ...`. Inside `bulk command` `for item in items` loops, `collect_fenced_bindings` and `write_evidence::fences` skipped `collection_local` / `repeated_instruction` bindings and `CollectionElementField` comparisons, forcing `checked_by <req>` on every `set` line. In addition, `RDB-C048` and `RDB-C050` help strings did not show concrete syntax examples.
4. **Manual query chunking in application code:** Every RiffQL query enforces a compile-time `take N` cap and bounded `Set<T>` / `Set<T, MAX>` parameter cardinality (`<= 200`). While the database-side bound is essential, generated SDKs did not expose the compiled cap constants or provide a bounded `<query>All` helper to chunk `Set<T>` inputs and iterate `nextCursor` pages.

## Decision

1. **Null-coalescing expression (`a ?? b` and `coalesce(a, b)`) and omissible `optional<T>` SDK command inputs:**
   - Extend `.riff` expression syntax (`crates/riffdb-contract-syntax`), compiler typechecking (`crates/riffdb-contract-compiler`), executable IR (`crates/riffdb-contract-ir`, `BinaryOperator::Coalesce`), and deterministic evaluation (`crates/riffdb-invariant`) with right-associative `lhs ?? rhs` and `coalesce(lhs, rhs, ...)`.
   - Type rule: `lhs` must have type `optional<T>`, and `rhs` must have type `T` (producing `T`) or `optional<T>` (producing `optional<T>`) with identical scalar type and bounds. At runtime, if `lhs` evaluates to `Value::Null`, `BinaryOperator::Coalesce` evaluates and returns `rhs`; otherwise it returns the unwrapped `T` (or non-null `optional<T>`) value of `lhs`. Contracts that do not use `Coalesce` retain their existing least-sufficient grammar and executable IR versions so bundle hashes remain byte-identical.
   - In generated TypeScript, Go, and Python SDKs (`crates/riffdb-query-module`), command input fields of type `optional<T>` become optional in caller input types (`readonly field?: T | null` in TypeScript, `field: T | None = None` in Python) and encode omitted/`undefined` fields as `null`.
2. **Iteration-local `CallerVersionFence` recognition in `bulk command` `for` loops and actionable `RDB-C048`/`RDB-C050` help:**
   - In `crates/riffdb-contract-compiler/src/write_guard.rs` and `crates/riffdb-contract-ir/src/write_evidence.rs`, recognize `WriteEvidence::CallerVersionFence` inside `for` loops when: (a) the mutated binding (or its aggregate root bound in the same loop scope) is a `mutate` binding, (b) a dominating `require <name>: <root>.revision == <input_or_element_field>` in the same loop scope precedes the `set` instruction, and (c) the loop body advances `<root>.revision = <root>.revision + 1`.
   - Update `RDB-C048` (`UnguardedSet`) and `RDB-C050` (`InvalidCheckedByAssociation`) help text in `crates/riffdb-contract-compiler/src/diagnostic.rs` to show exact copy-pasteable `require` and `checked_by` syntax examples.
3. **Generated SDK `<query>All` helpers and query bound constants (`crates/riffdb-query-module`):**
   - Emit compile-time bound constants per query (effective `take` limit and any `Set<T>` parameter maximum) and generate `<query>All(params, options?)` across TypeScript, Go, and Python clients.
   - For queries with a single `Set<T>` parameter (such as `TodosByIds`), `<query>All` partitions the input set into chunks of at most the parameter's compiled cap (`<= 200`), drains each chunk, and concatenates rows in chunk order. For cursor-paginated queries, `<query>All` follows `nextCursor` up to a bounded `maxPages` (default `64`, hard cap `1024`, failing closed on exhaustion). Across all chunks and pages after the first response, `<query>All` pins `readAfterCommit` to the highest observed `applicationHead` unless the caller already supplied an explicit `readAfterCommit`.
4. **Contract & RiffQL pattern guide (`docs/contracts/README.md`, `docs/contracts/COMMAND-INVARIANT-COOKBOOK.md`, `docs/riffql/LANGUAGE.md`):**
   - Rewrite `docs/contracts/README.md` as a practical, pattern-first guide covering entities, `optional<T>`, aggregates, create, update with revision check, partial update with `??` / `coalesce`, delete, bulk command loops, and matching `.riffq` queries, positioning `docs/contracts/LANGUAGE.md` clearly as the formal grammar appendix.

## Options considered

- **Macro-expanding partial updates into multiple commands:** Rejected because $N$ optional fields would require $2^N$ command variants and inflate operation catalogs.
- **Unbounded server-side queries:** Rejected because unbounded scans violate RiffDB's bounded-execution guarantee (`QRY-001`, `QRY-002`); client-side bounded chunking/pagination with `readAfterCommit` pinning preserves server bounds and ergonomics.

## Consequences

- Single `Update<Entity>` commands can express partial updates safely under a revision fence without proliferating per-field commands.
- Bulk commands over aggregate roots get the same automatic `CallerVersionFence` recognition as unary commands.
- Existing contract bundle hashes and wire formats remain unchanged for contracts not using `??` / `coalesce`.

## Standing design tests

- **Interface safety:** `??` / `coalesce` requires exact scalar type and bound equality between `optional<T>` and fallback `T`, checks only `Value::Null` (never falsy values), and still requires every `set` to satisfy write-guard verification (`CallerVersionFence` or `BusinessCheck`). `<query>All` enforces a hard cap on pages/chunks (`<= 1024`) and pins `readAfterCommit`.
- **Scale:** `BinaryOperator::Coalesce` adds $O(1)$ per-node evaluation overhead within existing expression depth/node budgets, and `<query>All` bounds each RPC to the query's compiled `take` and `Set<T>` limits.

## Checks

- Proof tests `partial_update_coalesce_preserves_unspecified_fields_under_revision_fence`, `bulk_loop_caller_version_fence_guards_element_mutations_without_checked_by`, and `generated_sdk_query_all_chunks_bounded_sets_and_paginates_cursors`.
- `./scripts/check-adr-form`, `./scripts/check-adr-obligations`, `./scripts/check-generated`, `./scripts/handbook check`, and `./scripts/acceptance --wp WP-924`.
