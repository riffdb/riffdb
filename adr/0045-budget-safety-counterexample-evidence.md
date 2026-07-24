# ADR-0045: Budget Safety Counterexample Evidence and Claim Boundary

- **Status:** Accepted
- **Direction approved:** 2026-07-23 for adding comparison evidence; exact
  scenarios and wording were accepted on 2026-07-23
- **Exact text accepted:** 2026-07-23
- **Accepted:** 2026-07-23
- **Acceptance reference:** Human maintainer confirmation in the current Codex
  session on 2026-07-23
- **Requires:** ADR-0005, ADR-0007, ADR-0015, ADR-0031, ADR-0037, and
  ADR-0041
- **Amends:** SPEC Sections 5.1, 16.1, 17.8, 19.3 through 19.5, 20.2
  through 20.4, and 23.4; the ADR register; the work-package registry; WP-200
  dependencies and evidence; and the work-package and roadmap diagrams
- **Decision deadline:** Before comparison-owned source or fixtures are changed
  for the safety counterexamples

The human maintainer accepted this early executable comparison showing that
common application mistakes which remain expressible through general
PostgreSQL SQL are not expressible through RiffDB's supported application
mutation surface. This record fixes the exact bounded claim, evidence ownership,
scenarios, and exclusions.

## Context

The long-lived budget comparison currently has:

- a deliberately correct PostgreSQL implementation using explicit
  `READ COMMITTED` transactions, `SELECT ... FOR UPDATE`, exact decimals,
  synchronous commit, and row checks;
- an in-process RiffDB application-service implementation;
- a public Rust SDK and gRPC implementation; and
- a closed `riffdb.budget.public-run/v1` child-process protocol with
  `sequential`, `contention`, and `same_key_replay` cases.

That evidence proves workload parity and records differing guarantee profiles,
but it does not make the protocol-enforcement value visible. The PostgreSQL
adapter's correct implementation can obscure the fact that an application with
the same table credential can omit a lock or precondition, ignore an
idempotency key, or issue direct DML.

The comparison must remain technically honest. PostgreSQL supports safe
implementations using row or advisory locks, conditional writes,
`SERIALIZABLE`, restricted table privileges, stored procedures, idempotency
tables, triggers, and transactional event or outbox tables. The claim is not
that PostgreSQL is inherently unsafe or cannot reproduce a RiffDB guarantee.
The claim concerns which obligations the supported application interface makes
optional.

The existing PostgreSQL adapter is also the future fair benchmark baseline.
Deliberately incorrect variants must not contaminate it or be presented as
performance peers.

## Proposed Decision

### Claim boundary

Every generated report, human-readable rendering, README statement, and demo
must preserve this claim:

> PostgreSQL supports safe implementations, including the canonical comparison
> adapter. The counterexamples show that hazardous patterns remain expressible
> through general SQL and host transaction code, while the corresponding
> patterns are absent or rejected through RiffDB's supported application
> mutation and compiled-contract surfaces.

The shorter phrase "impossible in RiffDB" may be used only when immediately
qualified as "not expressible through RiffDB's supported application mutation
surface."

The evidence does not claim protection against:

- an incorrectly specified or maliciously deployed contract;
- an authorized administrator changing a contract or capability;
- direct operating-system, database-file, or process compromise;
- bugs in RiffDB's implementation; or
- features outside the accepted POC model.

### Work-package ownership and sequencing

Register one non-gating evidence package:

**WP-139: Budget safety counterexample evidence**

- Hard dependencies: `WP-045` and `WP-135`.
- Required ADR: this ADR after acceptance, plus the accepted semantic and
  public-boundary ADRs already required by those packages.
- Gate membership: none. It remains a dashed comparison-evidence track and does
  not become a P0, P1, or P2 implementation gate.
- Downstream dependency: add `WP-139` as a hard dependency of `WP-200`, because
  the final POC report and demo must consume the checked safety report.
- Current work may be paused operationally until WP-139 passes, but WP-140 and
  WP-150 do not gain false semantic dependencies on comparison evidence.

WP-139 receives narrow additive authority over comparison paths otherwise
owned by WP-045 and WP-135. It must not rewrite their accepted evidence:

- the canonical `riffdb-budget-comparison-postgres` implementation, guarantee
  profile, workload, and existing fixtures remain unchanged;
- the closed `riffdb.budget.public-run/v1` grammar, binary behavior, case set,
  and exact fixtures remain unchanged;
- existing benchmark-eligible adapters continue to implement only the shared
  correct workload; and
- the new negative controls never implement the normal `BudgetBackend` trait.

The package may add:

- `examples/budget-comparison/safety-evidence/**`;
- `examples/budget-comparison/fixtures/safety/**`;
- `examples/budget-comparison/tests/safety_*`;
- the existing
  `examples/budget-comparison/tests/postgres_isolation.rs` membership assertion
  only, to replace its comment-matching false positive with an exact manifest
  member check;
- additive public-adapter evidence methods in
  `examples/budget-comparison/riffdb-grpc/src/lib.rs`;
- nested workspace manifest and lockfile rows;
- comparison README and CI coverage; and
- one dedicated `scripts/budget-safety-demo` entry point.

No production crate, root production dependency, public Protobuf message, RPC,
contract grammar, typed IR, plan hash, canonical input hash, durable record,
storage key, or accepted budget contract changes.

### Evidence architecture

Add a safe Rust nested-workspace package named
`riffdb-budget-safety-evidence`. It may contain a library, a fixture checker,
and a runnable evidence binary. It uses the existing pinned dependency graph.
WP-139 adds no new third-party package, version, or feature; new direct rows may
only reuse exact dependencies already resolved in the nested workspace, and the
resolved lock diff remains subject to dependency review.

Its normal direct dependencies are limited to:

- `postgres = 0.19.14`, default features disabled;
- the comparison `core`, canonical `postgres`, and public `riffdb-grpc` path
  packages;
- `riffdb-client-rust`, default features disabled;
- `serde_json = 1.0.150`, default features disabled with `std`;
- `tokio = 1.52.0`, default features disabled with only the already-resolved
  `macros`, `rt-multi-thread`, and `time` features; and
- `tonic = 0.14.6`, default features disabled with only the already-resolved
  `channel` and `codegen` features.

The root comparison package may continue to use its existing test-only
`riffdb-auth` and server-composition dependencies. The safety package gains no
direct auth, server, service, storage, policy, commit, runtime, compiler, or MCP
dependency.

The package owns a conspicuously named PostgreSQL negative-control module. That
module may connect to the existing dedicated comparison database and issue the
scenario SQL, but it must not:

- be reachable through the canonical PostgreSQL adapter's normal workload
  trait;
- export a benchmark operation;
- emit latency or throughput comparisons;
- appear in the canonical PostgreSQL guarantee profile; or
- be linked into the root RiffDB workspace.

The process test may use the existing WP-135 test-only
`riffdb-auth::bootstrap_secret` harness to create protected startup
configuration, start a fresh `riffdbd`, deploy the accepted Budget contract
through the public bootstrap flow, and issue one normal capability. After that
provisioning boundary, every scenario application operation and observation
uses only the public Rust SDK and gRPC surface. The normal capability is limited
to `InvokeCommand` for command IDs 1 and 2, `ReadEntity` for entity ID 1,
`SubscribeCommits`, and `ScanCommits`. The command-specific `InvokeCommand`
grants also authorize outcome resolution.

The RiffDB side does not call the API-neutral service directly, access storage,
seed entity records outside `CreateBudget`, or add a privileged comparison
path.

The PostgreSQL lost-update schedule is deterministic through explicit
read/update/commit controller messages. RiffDB contention uses an explicit
client-side start barrier and bounded concurrent submissions; it proves the
safe public result, not a controlled or explored internal server schedule. No
correctness assertion uses a sleep.

### Closed scenario set

WP-139 contains exactly these four scenarios, in this order.

#### 1. `lost_update_without_lock`

Seed one budget with approved amount `100.00` and allocated amount `0.00`.

The PostgreSQL negative control opens two independent `READ COMMITTED`
transactions. Each reads `0.00` without `FOR UPDATE`, computes an absolute
post-image of `80.00`, and reports that the business allocation is acceptable.
The controller waits until both reads complete, releases the first absolute
update and commit, then releases the second absolute update and commit. Both
handlers report allocation success, both table checks hold, and final durable
allocation is `80.00` even though `160.00` was logically accepted.

The RiffDB comparison releases the two existing `AllocateBudget(80.00)`
commands with different idempotency keys against the same conflict domain.
Both terminalize as committed declared outcomes. Exactly one returns
`Allocated`; its public commit has one affected entity and one
`BudgetAllocated` event. Exactly one returns `InsufficientBudget`; its public
commit has zero affected entities and zero events. Final allocation is
`80.00`.

This provides bounded application-protocol evidence toward `TXN-001` and
`POC-002`. It does not claim that PostgreSQL with `FOR UPDATE`, a correct
conditional update, or `SERIALIZABLE` loses the update. The unchanged canonical
PostgreSQL adapter continues to demonstrate the `FOR UPDATE` remedy.

#### 2. `direct_dml_precondition_bypass`

Seed a `100.00` budget and allocate `30.00` through the canonical path.

The canonical PostgreSQL adapter must return `InvalidAmount` for a
`-10.00` allocation. The negative control then uses the same comparison
application database authority to issue direct DML equivalent to adding
`-10.00`. The update commits, final allocation becomes `20.00`, and the
existing row checks still hold. The operation has bypassed the command
precondition even though it did not violate the weaker table checks.

The RiffDB public client submits `AllocateBudget(-10.00)`. Execute returns
transport success with completion `Committed` and declared `InvalidAmount`.
The zero-mutation rejection has its own commit sequence and provenance locator;
its public commit has zero affected entities and zero events; and state remains
`30.00`. Descriptor and client-inventory tests prove the absence of a generic
application insert, update, or delete RPC. Accepted ADR-0007 and existing
service architecture evidence, rather than RPC naming alone, establish that
authoritative application entity mutation routes through
`CommandService.Execute`.

This provides bounded evidence toward `SYS-004`, `DSL-009`, `API-001`,
`POC-003`, and `POC-008`. It does not claim that PostgreSQL table privileges
plus a reviewed stored procedure cannot establish a similar application
boundary.

#### 3. `duplicate_retry_after_discarded_response`

Seed a `100.00` budget. Submit `AllocateBudget(30.00)` with idempotency key
`K`, allow it to commit, deliberately discard the returned application result,
and submit the identical logical request again.

The PostgreSQL comparison adapter intentionally has no idempotency
implementation and ignores `K`. Both calls return `Allocated`, two allocation
updates commit, and final allocation is `60.00`.

RiffDB returns the original `Allocated` outcome with `replayed=true`; the
replay response matches the one public commit's sequence, plan hash, provenance
URI, and declared outcome name/value. A public `GetOutcome` resolution returns
the same declared outcome and the replay response's outcome locator. The
matching commit has one affected entity and one `BudgetAllocated` event, and
final allocation is `30.00`.

This provides bounded evidence toward `OUT-001`, `OUT-004`, and `POC-005`. It
is deliberately discarded-response evidence, not the synchronized TCP-loss and
process-restart evidence required by `POC-004`. WP-190 and WP-200 retain
ownership of that failpoint.

#### 4. `same_key_different_input`

Seed a `100.00` budget. Submit `AllocateBudget(30.00)` with idempotency key
`K`, then submit `AllocateBudget(40.00)` with the same identity.

The PostgreSQL adapter ignores `K`, commits both mutations, and finishes at
`70.00`.

RiffDB commits the first command, rejects the second with
`PublicErrorKind::IdempotencyKeyReuse`,
`PublicErrorDetails::None`, and no incident ID. State remains `30.00`.
An exact-end public commit scan proves that the application frontier did not
advance for the mismatch and that only one matching allocation commit exists;
that commit has one affected entity and one `BudgetAllocated` event. Canary
tests prove the prior input and raw idempotency key are absent from the report,
stdout, stderr, public error, and debug rendering.

This provides bounded evidence toward `OUT-002` and `POC-005`.

### Machine-readable evidence

Freeze one deterministic UTF-8 JSON fixture and one exact JSONL success record
under the identifier:

```text
riffdb.budget.safety-evidence/v1
```

The report has exactly these top-level fields and fixed values:

- `schema`: `"riffdb.budget.safety-evidence/v1"`;
- `workload_version`: `1`;
- `claim_scope`: `"supported_application_mutation_surface"`;
- `postgres_control`: `"canonical_adapter_preserved"`;
- `riffdb_surface`: `"public_rust_sdk_over_grpc"`;
- `benchmark_eligible`: `false`; and
- `scenarios`: four tagged scenario objects in the order specified above.

Each scenario object has exactly `scenario`, `postgres_negative_control`,
`riffdb_public`, and `claim`. Object keys use the existing comparison fixture's
lexicographic canonical ordering; the scenario array uses the fixed order
above. All counts are nonnegative JSON integers, all amounts are exact
two-fractional-digit strings, and all enum-like values use the exact
case-sensitive strings below.

`lost_update_without_lock` has claim
`"callers_cannot_select_weaker_conflict_handling"`.
Its PostgreSQL object has exactly:

- `accepted_count`: `2`;
- `canonical_adapter_oracle_passed`: `true`;
- `final_allocated_amount`: `"80.00"`;
- `logical_accepted_amount`: `"160.00"`; and
- `row_checks_hold`: `true`.

Its RiffDB object has exactly:

- `allocated_commit_affected_entity_count`: `1`;
- `allocated_commit_event_count`: `1`;
- `allocated_count`: `1`;
- `final_allocated_amount`: `"80.00"`;
- `insufficient_budget_commit_affected_entity_count`: `0`;
- `insufficient_budget_commit_event_count`: `0`;
- `insufficient_budget_count`: `1`; and
- `terminal_declared_outcome_count`: `2`.

`direct_dml_precondition_bypass` has claim
`"declared_precondition_cannot_be_omitted_or_bypassed"`.
Its PostgreSQL object has exactly:

- `direct_dml_committed`: `true`;
- `final_allocated_amount`: `"20.00"`;
- `requested_amount`: `"-10.00"`;
- `row_checks_hold`: `true`;
- `safe_adapter_outcome`: `"InvalidAmount"`; and
- `starting_allocated_amount`: `"30.00"`.

Its RiffDB object has exactly:

- `completion`: `"Committed"`;
- `final_allocated_amount`: `"30.00"`;
- `generic_application_dml_rpc_present`: `false`;
- `outcome`: `"InvalidAmount"`;
- `rejection_commit_affected_entity_count`: `0`;
- `rejection_commit_event_count`: `0`;
- `rejection_has_commit_sequence`: `true`; and
- `rejection_has_provenance_uri`: `true`.

`duplicate_retry_after_discarded_response` has claim
`"same_request_replays_without_duplicate_mutation"`.
Its PostgreSQL object has exactly:

- `committed_allocation_count`: `2`;
- `final_allocated_amount`: `"60.00"`;
- `first_outcome`: `"Allocated"`; and
- `second_outcome`: `"Allocated"`.

Its RiffDB object has exactly:

- `allocation_commit_affected_entity_count`: `1`;
- `allocation_commit_event_count`: `1`;
- `final_allocated_amount`: `"30.00"`;
- `matching_allocation_commit_count`: `1`;
- `replay_completion`: `"Replayed"`;
- `replay_outcome_lookup_matches`: `true`;
- `replay_same_commit_sequence`: `true`;
- `replay_same_declared_outcome`: `true`;
- `replay_same_plan_hash`: `true`; and
- `replay_same_provenance_uri`: `true`.

`same_key_different_input` has claim
`"same_identity_different_input_fails_without_execution"`.
Its PostgreSQL object has exactly:

- `committed_allocation_count`: `2`;
- `final_allocated_amount`: `"70.00"`;
- `first_amount`: `"30.00"`; and
- `second_amount`: `"40.00"`.

Its RiffDB object has exactly:

- `allocation_commit_affected_entity_count`: `1`;
- `allocation_commit_event_count`: `1`;
- `final_allocated_amount`: `"30.00"`;
- `matching_allocation_commit_count`: `1`;
- `mismatch_error_details`: `"none"`;
- `mismatch_error_kind`: `"idempotency_key_reuse"`;
- `mismatch_incident_id_present`: `false`;
- `post_mismatch_application_frontier_unchanged`: `true`; and
- `secret_canary_absent`: `true`.

No report object contains other fields. In particular, it does not contain
elapsed time, host paths, credentials, database URLs, random request IDs, raw
idempotency keys, incident sources, or unstable process metadata.
Serialization uses typed observation structs and `serde_json` values with
canonically ordered maps rather than ad hoc JSON string construction. The
pretty JSON fixture ends with one LF. The success JSONL fixture is the compact
encoding of the same value followed by one LF. Both parse back to the equal
typed report and are limited to 32,768 bytes.

### Runner and demo contract

The runnable binary is `riffdb-budget-safety`. It accepts exactly these eight
flag/value arguments in this order:

```text
--protocol riffdb.budget.safety-evidence/v1
--postgres-url-file <PATH>
--endpoint <LOOPBACK_HTTP_URL>
--credential-file <PATH>
```

This is an isolated, non-shipped comparison evidence artifact like
`riffdb-budget-public`. It is not a fourth RiffDB product binary and is never
included in release packaging.

It accepts no ambient configuration. Total encoded argv is at most 12,288
bytes; the endpoint is at most 512 ASCII bytes and must be loopback HTTP; each
path is nonempty, NUL-free, and at most 4,096 platform-encoded bytes. The
protected bearer loader remains owned by the public client. The
PostgreSQL-URL file loader accepts one 1-through-4,096-byte UTF-8 URL with no NUL
or line terminator and, on Linux, requires a stable regular nonsymlink file
owned by the effective user with no group or other permission bits.

On success the binary writes the exact compact report JSONL to stdout, nothing
to stderr, and exits `0`. A checked scenario, connection, timeout, protocol, or
output failure writes exactly `riffdb budget safety evidence failed\n` to
stderr, nothing to stdout, and exits `1`. Invalid argv or protected-file input
writes exactly `riffdb budget safety invocation invalid\n` to stderr, nothing
to stdout, and exits `2`. Neither stdout nor stderr may exceed 32,768 bytes.

The process harness owns fresh-server provisioning and passes only the normal
credential to this binary. It applies bounded 15-second startup, 10-second RPC,
180-second runner, 10-second graceful-stop, and 5-second forced-stop timeouts.

`scripts/budget-safety-demo` accepts exactly `--assert`. It requires the
existing `RIFFDB_BUDGET_POSTGRES_URL` environment variable for a dedicated
PostgreSQL database, builds the exact root `riffdbd` and nested safety runner,
and runs the required-live process test that provisions a fresh RiffDB
database. Missing PostgreSQL connectivity, failed RiffDB startup/readiness, a
skipped test, or any non-passing scenario is a bounded nonzero failure. On
success it presents the checked report; it never emits the PostgreSQL URL,
bearer credential, bootstrap credential, or temporary paths.

The runner grammar and bytes are comparison-evidence interfaces, not RiffDB
public protocol or durable formats. Changing them, the report identifier,
common or scenario-specific field inventory, exact claim values, bounds, exit
codes, or scenario order requires human review.

## Options Considered

1. **Modify the canonical PostgreSQL adapter to include unsafe modes.**
   Rejected because the adapter is the correct semantic and future benchmark
   baseline. A mode flag could accidentally mix invalid runs into comparative
   performance results.
2. **Extend `riffdb.budget.public-run/v1` with more cases.** Rejected because
   ADR-0041 freezes its closed case set and WP-150 consumes its exact bytes. A
   distinct safety report avoids an incompatible change.
3. **Add a second cross-row contract and demonstrate write skew.** Deferred
   because the current request can be proved with the accepted single-budget
   contract. A new contract would broaden the example and create language,
   compiler, generated-client, and dependency-validation review.
4. **Demonstrate arbitrary external effects escaping rollback.** Deferred
   until the accepted outbox path is composed in WP-160 and WP-185. A
   process-global test stand-in would be less persuasive than end-to-end outbox
   evidence.
5. **Claim real post-commit connection loss in this package.** Rejected.
   WP-139 deliberately discards a received response. WP-190 owns synchronized
   TCP-loss and process-restart proof.
6. **Wait until WP-200.** Rejected because early executable counterexamples
   provide architectural feedback before the POC is packaged and benchmarked.

## Consequences

- The example communicates the contract-first value proposition with
  executable, reviewable observations rather than only a feature matrix.
- Correct PostgreSQL implementation and remediation remain visible beside each
  negative control.
- The public RiffDB path, not an in-process shortcut, proves the comparison.
- Future benchmarks cannot accidentally include deliberately incorrect
  variants.
- A small additional non-production package, report interface, live CI job,
  and process harness must be maintained.
- WP-200 cannot complete without consuming the safety report.
- This decision provides no new RiffDB semantic capability and satisfies no
  part of the deferred outbox, projection, or real connection-loss work.

## Compatibility

The proposal is additive outside production interfaces. It changes no public
Protobuf symbol, field, enum, RPC, generated client operation, contract source,
IR, plan hash, canonical input hash, durable envelope, storage key, or on-disk
format.

The existing comparison workload, canonical PostgreSQL adapter, guarantee
profile, public runner protocol, CLI fixture, and benchmark eligibility remain
byte-for-byte stable. The new report starts at version 1 and has no compatibility
promise beyond checked repository evidence; changing its identifier, common
field set, scenario set, order, or claim wording requires human review.

## Security

- PostgreSQL negative controls run only against the dedicated destructive-test
  database already required by the comparison workspace.
- Database URLs and credentials are bounded and redacted from all output.
- RiffDB cases use normal public authentication, authorization, command
  execution, commit, and provenance paths.
- The protocol-inventory check proves no generic application entity-write RPC;
  it does not treat administrative deployment or capability operations as
  application DML.
- Idempotency mismatch output contains no prior canonical input, raw key, token,
  or authority-bearing identifier.
- The new package remains `#![forbid(unsafe_code)]` and cannot become a
  production dependency.

## Testing

WP-139 must provide:

- offline unit tests for report construction, canonical order, bounds,
  redaction, and fixture reproduction;
- a required-live PostgreSQL test that verifies
  `server_version_num=180004`, transaction and wait settings, durability
  settings, and every negative-control observation;
- a CI architecture assertion that the workflow retains the exact
  digest-pinned PostgreSQL image row;
- a real-`riffdbd` process test using the bounded bootstrap exception described
  above and only the public Rust SDK and gRPC path after provisioning;
- explicit scheduling assertions for both pre-update reads and both commits in
  the PostgreSQL lost-update case, plus an explicit RiffDB client start barrier,
  with no claim of an internally controlled server schedule and no sleep-based
  correctness;
- state and typed-outcome assertions plus exact matching-commit counts,
  `affected_entities.len()`, `events.len()`, event type, replay identity,
  exact-end commit frontier, and mismatch assertions appropriate to each
  scenario;
- an architecture test that no negative-control type implements or is passed as
  `BudgetBackend`, no benchmark target depends on the safety-evidence package,
  no safety code emits performance measurements, canonical adapter
  sources/profiles remain unchanged, and PostgreSQL remains absent from
  production workspace dependencies;
- a corrected exact nested-workspace member assertion that inspects the actual
  `members` assignment rather than matching comments;
- a descriptor/client-inventory test for the absence of generic application-DML
  RPCs, paired with the existing ADR-0007 service architecture evidence for
  command-only mutation routing;
- exact report, runner-success, checked-error, and invalid-invocation fixtures;
- nested-workspace formatting, Clippy, tests, docs, dependency policy, and
  fixture-generation checks; and
- a runnable `scripts/budget-safety-demo --assert` path that fails closed when
  PostgreSQL connectivity or fresh `riffdbd` startup/readiness is unavailable.

The accepted PostgreSQL image remains:

```text
postgres:18.4-bookworm@sha256:d9c83446333daec3f0588cc709adb80c26090b7f9f0f7ec8d43c243385d79818
```

No skipped live test is WP-139 exit evidence.

## Requirements and Work Packages

- **Requirements:** `SYS-004`, `DSL-009`, `OUT-001`, `OUT-002`, `OUT-004`,
  `TXN-001`, `API-001`, `POC-002`, `POC-003`, `POC-005`, and `POC-008`
- **Defines or blocks:** `WP-139` and its future `WP-200` dependency
- **Final evidence:** `WP-139` for the bounded comparison claim; `WP-190` and
  `WP-200` for real post-commit connection-loss, restart, and complete POC
  evidence

WP-139 does not close the exclusive-capability mechanism of `TXN-001`, the
generated-history universal quantifier of `POC-003`, the crash/restart evidence
of `POC-005`, or the all-transport quantifier of `POC-008`. Their existing
semantic, recovery, and final-acceptance owners remain unchanged.

## Decision Deadline

Exact acceptance is required before:

- adding WP-139 to `work_packages.yaml`;
- changing any comparison-owned source, manifest, lockfile, fixture, README, CI,
  or diagram for this evidence;
- freezing scenario-specific report fields or runner bytes; or
- presenting any counterexample as an architectural result.

Acceptance does not pre-approve a changed third-party package, version, feature,
or third-party resolved graph; a new contract; a production interface change;
or any claim outside the boundary above.
