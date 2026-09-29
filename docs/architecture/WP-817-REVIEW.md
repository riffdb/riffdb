# WP-817: Atomic indexed selection

ADR-0264 was accepted verbatim by the maintainer before implementation. Work is
isolated on `feat/atomic-indexed-selection`; maintainer review precedes merge.

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
rejected because selection would disclose their membership/order. Raw stored
rows reconstruct index entries before catalog normalization; runtime retains the
original physical range evidence when historical optional fields become null.

## Semantic evidence

- Compiler fixture, exact-key V25 control, V26 round trip/hash distinction,
  source-span diagnostic, malformed ownership proof, aliases/multiple selectors,
  secret membership/idempotency taint, and selected-child root coverage.
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
validation, without an exclusive performance host or a stability claim.

Every case completed 32 distinct allocations with 32 selection reads, 32 inspected
live entries, a maximum of one candidate per evaluation, and zero retries. The
ordered execution path explains the zero retries; deterministic tests separately
force changed-generation reevaluation. Lock-wait observations cover complete
conflict-set acquisition. The entry counter measures live entries after the
bounded checkpoint/journal merge, not raw disk blocks or overlay tombstones.

At population 128:

| Clients | Shared commands/s | Independent commands/s | Shared p95 ms | Independent p95 ms |
|---:|---:|---:|---:|---:|
| 1 | 282.3 | 201.6 | 1.74 | 4.43 |
| 8 | 215.8 | 233.7 | 37.87 | 36.17 |
| 32 | 224.5 | 229.6 | 142.08 | 138.89 |

Reproduce from the repository root, using an absolute output path or one relative
to `crates/riffdb-commit` (Cargo's test working directory):

```bash
RIFFDB_WP817_EVIDENCE=../../docs/architecture/evidence/wp817-selection.json \
cargo test --release -p riffdb-commit --test command_concurrency \
  support::indexed_selection::concurrent_selection_has_bounded_work_in_shared_and_independent_pools \
  -- --exact
```

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
- Checks: targeted compiler, generated-surface, coordinator, redb concurrency,
  uncertainty, and process-crash tests pass; scoped clippy passes. Package
  acceptance and full CI are running; package closure is not yet claimed.
- Compatibility: additive V26 selector plans; old artifacts remain byte exact.
- Handbook: atomic selection guide, cookbook, known limitations, language
  reference, SUMMARY; generated IR format, diagnostic and compiler fixtures.
- Hazards: limitations above; maintainer review before merge. Remove the worktree
  after the reviewed merge to main.
