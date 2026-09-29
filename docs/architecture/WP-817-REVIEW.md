# WP-817: Atomic indexed selection

ADR-0264 was accepted verbatim by the maintainer before implementation. Work is
isolated on `feat/atomic-indexed-selection`, rebased onto main at `d3f0a2102`
(including WP-814 journal-frame V3); maintainer review precedes merge. Main
advanced to `e2af28320` during validation with another WP-814 journal change.
Rebase and revalidate that combined tree before merging; the results below cover
the recorded `d3f0a2102` base.

## Behavior and boundaries

`mutate first Entity.index(prefix)` selects and mutates one indexed candidate in
one ordinary command. The compiler proves input-only prefix, partition, and the
complete upfront conflict union. Storage seeks the prefix and captures its first
entry, entity, and invariant roots in one view. The coordinator validates the
partition/index generation and exact rows for successful, empty, rejected, and
execution-failure outcomes, with the existing three-attempt budget and identity.

Selection supplies no assignment evidence. Written-aggregate invariants,
capability/row/relationship policies, atomic events and provenance, idempotency,
and uncertainty fencing retain their ordinary owners. A selected-row policy is
also required before disclosing a deterministic execution fault. Empty results
are sticky; requirement failure or policy denial never skips a candidate.

Exact-target grouping, detached preparation, and deferred grouping exclude
selectors. Group durability uses ordered immediate execution and preserves
predecessor visibility. Private writer snapshots also implement the same closed
one-entry storage request. No storage encoding or public transport envelope
changes; selector contracts require grammar/IR/bundle V26, while nonselector
artifacts retain least-sufficient encoding and identity.

Canonical equality-prefix encodings are admitted. Secret-bearing indexes are
rejected because selection would disclose their membership/order. Index
reconstruction uses logical null for missing optional indexed or covered fields,
matching migrated indexes. Catalog materialization proves the exact
ancestry that authorizes those omissions and retains raw dependency hashes;
current-writer omissions still fail closed.

## Semantic evidence

- Compiler fixture, exact-key V25 control, V26 round trip/hash distinction,
  source-span diagnostic, malformed ownership proof, aliases/multiple selectors,
  secret membership/idempotency taint, and selected-child root coverage.
- Historical selected rows reconstruct migrated presence/cover indexes with
  logical null while preserving raw evidence; unauthorized omissions refuse.
- Deterministic scripted snapshots cover selected/empty epoch invalidation,
  changed candidate on retry, three-attempt exhaustion, cancellation, substituted
  row refusal, selected-child invariant failure, and row-policy denial for
  mutations, declared rejection, and deterministic faults.
- Real redb exercises canonical allocation, release, sticky empty replay,
  first-candidate requirement rejection, and shared/independent concurrent pools
  compared in commit order with an independent in-memory first-key oracle.
- A lost commit response resolves the original identity before another allocation.
  Real child-process aborts before and after commit reopen as atomic entity,
  outcome, event, provenance, and exact idempotency replay.
- Generated Rust, Go, TypeScript, Python, and MCP interfaces expose only Allocate's
  declared request/pool/holder inputs and outcomes. Existing common-service and
  transport conformance remains part of full CI.

## Measurements

[Machine-readable release sample](evidence/wp817-selection.json) records fresh
populations of 32 and 128 rows, 32 allocations per case, and 1/8/32 clients in a
shared pool or 32 independent pools within one partition. Worker starts use a
barrier. Timings include capacity admission, allocation, durable acknowledgement,
and local client runtime setup. This is one local sample during repository
validation on an AMD Ryzen 9 7950X (32 logical CPUs, approximately 62 GiB
OS-reported RAM), without an exclusive performance host or a stability claim.

Every case completed 32 distinct allocations with 32 selection reads, 32 inspected
live entries, a maximum of one candidate per evaluation, and zero retries. The
ordered execution path explains the zero retries; deterministic tests separately
force changed-generation reevaluation. Lock-wait observations cover complete
conflict-set acquisition. The entry counter measures live entries after the
bounded checkpoint/journal merge, not raw disk blocks or overlay tombstones.

At population 128:

| Clients | Shared commands/s | Independent commands/s | Shared p95 ms | Independent p95 ms |
|---:|---:|---:|---:|---:|
| 1 | 292.7 | 240.5 | 4.67 | 5.34 |
| 8 | 226.2 | 196.4 | 53.50 | 57.02 |
| 32 | 207.0 | 229.4 | 154.16 | 139.15 |

Reproduce from the repository root, using an absolute output path or one relative
to `crates/riffdb-commit` (Cargo's test working directory):

```bash
RIFFDB_WP817_EVIDENCE=../../docs/architecture/evidence/wp817-selection.json \
cargo test --release -p riffdb-commit --test command_concurrency \
  support::indexed_selection::concurrent_selection_has_bounded_work_in_shared_and_independent_pools \
  -- --exact
```

## Validation

Package acceptance ran 2,711 tests successfully (one existing opt-in test skipped).
Its first run found stale editor highlight fixtures; those were regenerated, and
`./scripts/acceptance --wp WP-817 --no-tests` then passed all 17 remaining checks.
The final optional-field compatibility fix separately passed its catalog
regression, scoped clippy, 10 coordinator unit tests, all five redb selector
integration tests, and the release measurement above.

`./scripts/ci-all` passed workspace formatting, clippy, tests and doctests, archive
CLI checks, documentation, dependency/security checks, architecture checks,
operator acceptance, and governance checks before stopping at the same stale
fixture check. After regeneration, every command from `check-generated` through
the end of that script passed, including Helm checks, public-source authoring
acceptance, and source-install smoke. These are staged results, not a claim that
one uninterrupted CI invocation passed after the final fix.

Earlier parallel test launches used separate PID namespaces and collided through
the scratch-directory sweeper. Rerunning the broad suites in one PID namespace
removed that environmental interference. Supplemental tests used an isolated
temporary root. No test failure was ignored or test guarantee weakened.

Existing tool warnings remain: legacy ADR length warnings, mdBook's large search
index, cargo-machete's absent template source directories, and the system Python
module's maturin 1.15.0 versus the declared 1.14.1. The corresponding checks passed;
the repository's pinned tool declarations are unchanged.

## Risks and follow-ups

Pool design still controls contention. Partition/index generations remain
conservative across pools, and the ordered selector path offers no parallel
throughput, fairness, starvation, or disjoint-pool invalidation guarantee. The
bounded merge can inspect overlay transitions separately from its one live row.
Finer epochs and optimized grouping need separate proofs. General predicates,
late locks, cross-partition writes, cross-aggregate invariants, and durable
multi-command orchestration remain unavailable.

## PR note

- Package: WP-817. Tier: guarantee.
- Behavior: atomic first indexed candidate with current validation, bounded full
  retries, ordinary assignment evidence, policies, and durable outcome replay.
- Checks: package tests and all acceptance checks passed; all full-CI gates passed
  with the staged reruns and final compatibility checks described above.
- Compatibility: additive V26 selector plans; old artifacts remain byte exact.
- Handbook: atomic selection guide, cookbook, known limitations, language
  reference, SUMMARY; generated IR format, diagnostic and compiler fixtures.
- Hazards: limitations above; maintainer review before merge. Remove the worktree
  after the reviewed merge to main.
