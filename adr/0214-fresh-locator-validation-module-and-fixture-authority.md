---
adr: "0214"
title: Fresh-Locator Validation Module and Fixture Authority
status: proposed
tier: guarantee
date: 2026-09-07
accepted: null
requires: [ADR-0213]
amends: [ADR-0213]
supersedes: []
requirements: [OUT-001, OUT-002, TXN-042, REC-004, PERF-008, PERF-015, PERF-017, PERF-019, END-008]
packages: [WP-705]
obligations: []
review_triggers:
  - The extracted module would own commit, publication, fencing, storage access, durable bytes, public behavior, or anything beyond ADR-0213 Decisions 2 through 9 validation.
  - A production caller other than store.rs, a public or test re-export, population-sized state, repeated evidence traversal, or point, composite-successor, command-authority, or retained-history read would be introduced.
  - store.rs would grow relative to the WP-705 implementation base, or the module would gain serialization, configuration, hooks, dependencies, Cargo changes, or a durable or interface format.
  - An administration edit would escape the existing cfg(test) module, alter a named fixture beyond its raw corruption-publication shell, or broadly retier administration.rs.
  - The startup fixture would change production behavior, terminal bytes, census semantics, or assertions instead of registering its exact existing mutation.
  - The two ADR-0213 repair commits, transaction ordering, acknowledgement, failure, publication, fencing, restart, or evidence eligibility would change.
---
# ADR-0214: Fresh-Locator Validation Module and Fixture Authority

## Context

ADR-0213 authorizes two bounded fresh-locator repairs in four exact files. The
red-first implementation proved that keeping their private validation in
`store.rs` crosses the repository's 10,000-line production-file ceiling. A
private child module can hold the same validation while leaving `store.rs` as
the only transaction and publication orchestrator and making it no larger than
the WP-705 implementation base.

The combined suite also exposed four tests whose setup now violates the exact
permit that production correctly requires. Three deliberately corrupt physical
administration state through a recognized commit lane, while one valid terminal
execution-failure fixture omits its known mutation permit. The tests need exact
setup authority; production semantics do not need another repair.

## Decision

1. ADR-0213 Decision 1 is amended only to add these exact paths and purposes:
   `crates/riffdb-storage-redb/src/store/fresh_locator_validation.rs` solely for
   Decisions 2 through 9 private validation types and functions;
   `crates/riffdb-storage-redb/src/administration.rs` solely for Decision 6's
   three named `cfg(test)` corruption-fixture shells; and
   `crates/riffdb-storage-redb/src/startup/tests.rs` solely for Decision 7's one
   named valid fixture. The original four paths retain their exact purposes.
   All other WP-705 authority is unchanged; another file or purpose stops for
   a separately accepted amendment.

2. `fresh_locator_validation.rs` is a private child module of `store.rs` and
   contains only the private, bounded validation state and pure validation
   functions shared by the mandated ADR-0213 Decision 2 pre-arm repair and
   Decision 4 queued repair. `store.rs` remains the sole transaction, commit,
   successor-installation, witness-sealing, lane-submission, publication,
   disablement, and fencing orchestrator and the sole production caller. The
   module owns no database, transaction, root, port, lane, hook, or authority;
   it may only validate caller-owned bounded values and use existing codecs for
   the exact ADR-0213 evidence.

3. The module is private, its items have visibility no wider than `pub(super)`,
   and it receives no public, crate-public, or test re-export. It defines no durable or wire encoding, serialization
   contract, configuration, feature, hook, callback, dependency, or Cargo
   surface. It performs no point, composite-successor, command-authority,
   retained-history, population, filesystem, network, clock, or random read.
   Each pre-arm expected and actual permit inventory retains the existing
   `MAX_FRESH_LOCATOR_PRESERVING_MUTATIONS` bound of 8,194 entries. Queued
   state retains fixed ordinal slots bounded by the existing 256-transition
   and 16 MiB frame ceilings. The module retains no state after one validation
   and cannot acknowledge, publish, mutate, or convert uncertainty to absence
   or coverage.

4. Relative to WP-705's implementation base
   `186c96bc71c43f66c9096393fee5d26cde242318`, `store.rs` must have no positive
   net line growth after both ADR-0213 repairs. Validation moved into the child
   module is removed from `store.rs`, not duplicated. The exact post-seal and
   pre-submit call remains visible in `store.rs`. OBL-0213-4's existing proof
   must freeze both files, their private boundary, sole caller, exact dataflow,
   ordering, one-decode and one-pass bounds, complete fact inventory, and all
   prohibited reads and authority.

5. In the existing `administration.rs` `cfg(test)` module, only these tests may
   replace a `RedbWriteAccess` plus recognized `commit_for` corruption setup
   with `ports.shared.database.begin_write()` and the raw engine transaction's
   `commit()`:
   `redb_reactive_republish_fails_closed_on_an_allocator_skewed_stream`,
   `redb_reactive_republish_fails_closed_without_a_publication_record`, and
   `exact_empty_command_authority_rejects_a_command_audit_locator`. Raw commit
   is test-only authority to create deliberately invalid physical state; it is
   not a production lane and does not receive fresh-locator coverage.

6. Those three edits preserve the production prefix of `administration.rs` and
   each fixture's labels, seeds, keys, tables, encoded bytes, allocator skew,
   catalog or module setup, read path, rebuild observation, drops, error text,
   `CorruptData` assertions, and requirement tag byte-for-byte. Only the
   begin/access/commit shell changes. No production item, visibility, behavior,
   assertion, corruption shape, codec, or durable byte changes.

7. In `startup/tests.rs`, only
   `a_committed_terminal_execution_failure_advances_the_census_it_is_counted_by`
   may register `IDEMPOTENCY` and the already-derived exact terminal key as one
   expected byte insert, close that expected inventory, and record the same
   actual insert before performing the existing insert and
   `commit_execution_failure`. The failure value and key, transaction, commit,
   census, encoded-checkpoint, zero-walk, reopen, and all other assertions stay
   unchanged. This corrects valid test setup only; it changes no production
   execution-failure behavior or census rule.

8. The Decision 5 and 6 administration fixture edits and Decision 7 startup
   fixture edit belong to ADR-0213's first, pre-arm repair commit. The private
   module enters there with only the shared boundary and Decision 2 through 3
   validation needed by that repair. ADR-0213's queued Decision 4 repair remains
   a distinct second commit and adds its Decision 4 through 9 validator behind
   the same boundary. The second commit cannot absorb, reorder, or amend the
   first, and runtime evidence remains ineligible until both pass together.

9. OBL-0213-1 and its existing proof continue to own the complete pre-arm
   semantic and refusal matrix. OBL-0213-2 and its existing proof continue to
   own the queued exactness and linear-bound matrix. No new runtime obligation
   duplicates them. OBL-0213-4 and
   `production_fresh_locator_arming_site_is_reachable_from_both_command_batch_paths`
   additionally seal this record's paths, module boundary, sole caller,
   dataflow, sequencing, forbidden authority, non-growing `store.rs`, and
   fixture-only edits.

10. Only after exact-text human acceptance, its separate acceptance commit may
    add the exact new module path to the existing guarantee rule in
    `governance/tiers.yaml`, add ADR-0214 to `WP-705.required_adrs`, and
    regenerate required ADR status/index metadata. It must not add a glob,
    directory rule, or `administration.rs` rule, broadly retier a test file, or
    change package deliverables, acceptance commands, closure, or dependencies.
    This proposal commit changes only this record and its generated ADR index.

11. ADR-0213 otherwise remains exact: the direct and hardened helper, durable
    bytes, interfaces, hooks, configuration, dependencies, transactions,
    ordering, acknowledgement, visibility, cancellation, fencing, failure,
    restart, audit thresholds, and evidence rules do not change.

## Options considered

1. **Keep all validation in `store.rs`:** rejected; it breaches the enforced
   production-file ceiling and leaves two repairs in an already oversized unit.
2. **Create a general helper or public validation API:** rejected; no caller or
   authority outside the one storage orchestrator is needed or safe.
3. **Make corrupt fixtures satisfy a recognized permit:** rejected; their point
   is to construct impossible physical state below the recognized semantic lane.
4. **Use exact private extraction and fixture-only corrections:** chosen; it
   preserves ADR-0213 semantics, boundedness, commit split, and test intent.

## Consequences

- The two repairs share one narrow private validation boundary without growing
  the transaction orchestrator or adding another authority source.
- Corruption tests continue constructing the same invalid bytes honestly below
  production semantics, while the valid execution-failure fixture proves the
  permit required by its existing recognized lane.
- This record authorizes no new behavior, format, surface, hook, dependency,
  evidence claim, or general raw-write testing convention.

## Standing design tests

- **Interface safety:** no application, agent, operator, transport, test
  controller, or configuration can name the module, invoke validation, acquire
  storage authority, bypass a permit, reset coverage, or obtain a witness.
- **Scale:** each pre-arm inventory retains its existing 8,194-entry bound;
  queued slots remain fixed by the existing 256-transition and 16 MiB frame
  maxima. Queued validation remains one pass with no population or history read, and
  `store.rs` does not grow from its named base.

## Checks

- `fresh_locator_recognized_pre_arm_control_writes_preserve_uninitialized` and
  the three named administration tests plus the one named startup test prove
  the first repair and exact fixture intent without changed durable bytes.
- `fresh_locator_queued_frame_validator_is_linear_exact_and_bounded` proves the
  extracted queued validator's complete exactness and structural bound.
- `production_fresh_locator_arming_site_is_reachable_from_both_command_batch_paths`
  freezes both source files, sole-caller dataflow, commit ordering, forbidden
  authority, exact fixture scopes, governance path, and non-growing source.
- `./scripts/check-file-size-guard`, ADR/requirement/allowed-path checks, focused
  tests, and `./scripts/acceptance --wp WP-705` must pass before evidence runs.
