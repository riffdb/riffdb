---
adr: "0251"
title: A Declared Columnar Source Is Admitted Without A Restart
status: accepted
tier: guarantee
date: 2026-09-20
accepted: 2026-09-20
acceptance: 'maintainer, in session, 2026-09-20: "i approve of ADR-0251"
  (ADR-0251 as written); amended 2026-09-21, maintainer, in session, choosing
  "Amend ADR-0251 to record it unreachable" (Amendment 1, on the action as
  described)'
requires: [ADR-0250]
amends: []
supersedes: []
requirements: []
packages: [WP-802]
obligations:
  - id: OBL-0251-1
    package: WP-802
    proof: a_vector_field_deployed_into_a_running_daemon_becomes_queryable
    says: Deploying a contract that declares a vector field makes a projected
      query against it servable without restarting the process.
  - id: OBL-0251-2
    package: WP-802
    proof: a_vector_field_is_refused_before_compatibility_is_consulted
    says: Discharged as unreachable by Amendment 1. A source that cannot replay
      the history it needs is still refused at deploy in the implementation, but
      the scenario cannot arise. Reaching it needs a deploy that adds a
      columnar source to a database that already holds history, and no such
      deploy compiles. The named proof is the one that establishes that.
  - id: OBL-0251-3
    package: WP-802
    proof: an_unregistered_source_answers_with_a_typed_refusal
    says: A projected query against a source the engine does not hold answers
      with a typed refusal naming the condition, never an opaque internal defect.
review_triggers:
  - A contract-derived source would be resolved only at startup.
  - A deploy would be accepted that the running process cannot serve.
  - A projected query would fail as an internal defect for a reason the caller
    could have been told.
---
# ADR-0251: A Declared Columnar Source Is Admitted Without A Restart

## Context

Deploying a contract that declares a vector field into a running daemon
registers no columnar source. The deploy succeeds, the catalog carries the
field, and a projected query against it reaches a port that has never heard of
the source and fails as an internal defect with an opaque incident. Restarting
the process fixes it, because the source set is resolved during startup.

The source set is fixed by construction, not by convention.
`ColumnarRuntime::control_bindings` is a plain `BTreeMap` built in
`open_prepared`, and `engines` is only ever read after that. Nothing can add a
source to a running process because nothing is shaped to.

`prepare_columnar_control_foundation` does everything admission needs -- resolve
bindings from the active catalog, install a fresh control for each missing one,
reconcile, return the bindings -- and it runs once, before the writer exists.
Its comment says why: the all-or-nothing batch installs every fresh
`BeforeFirst` retention fence before any capability that could advance or prune
the log can open.

That precondition is what makes this a decision rather than a matter of calling
the same function later. A source admitted at runtime installs its fence while
the log is advancing and while pruning may already be running, and a fresh
source needs history from the beginning: a vector index over documents that
already exist is wrong if it starts at the current head.

One fact makes this tractable. Retention already honours these controls
dynamically -- `retention.rs` reads the `COLUMNAR_PROJECTION_CONTROLS` table in
its own precommit transaction and takes the minimum frontier across it, rather
than working from a set captured at startup. A fence installed at runtime is
therefore respected by the next pruning pass without any further wiring. The
only exposure is the window between installing the fence and that pass: history
the new source needs may already have been pruned.

**A deploy that the running process cannot serve should fail at deploy, not at
the first query.** That is the shape of the decision.

## Decision

1. **A contract-derived columnar source is admitted when its contract is
   deployed.** Deployment installs the fresh control, verifies the source can
   replay, registers the binding, and adds a cold engine slot to the running
   runtime. WP-777's demand activation then applies unchanged: the first
   projected query returns the building result and the population pass runs.

2. **Admission verifies replayability after installing the fence, and refuses if
   it cannot be met.** The control is installed at `BeforeFirst`, then retained
   history is checked to still begin where the source needs it. If pruning
   removed history in the window, the admission fails and the deploy is refused
   with a typed reason naming the condition. Installing first and checking after
   is deliberate: the fence is what stops the next pass, so it has to exist
   before the check means anything.

3. **A refused admission refuses the deploy.** A contract whose declared source
   cannot be admitted is not accepted with a broken source behind it. The
   operator learns at the point they acted.

4. **A projected query against a source the engine does not hold is a typed
   refusal.** It names the condition and is request-scoped in the sense ADR-0250
   gives that word. No path from an unregistered source to an opaque internal
   defect remains.

5. **Startup admission is unchanged.** `prepare_columnar_control_foundation`
   keeps its quiescent-point guarantee and stays the path for sources that exist
   when the process opens. Runtime admission is the addition, not a replacement,
   and the two agree on what a source is.

## Consequences

`ColumnarRuntime` gains the ability to hold a binding set that changes, which
means `control_bindings` moves behind the lock `engines` already uses. The
runtime's own doc comment, which says the engines and notifier are fixed at
startup registration, stops being true and is corrected with it.

A deploy can now fail for a reason that has nothing to do with the contract
being well formed. That is the cost of decision 3, and it is the right cost: the
alternative is the behaviour this record exists to remove, where the deploy
succeeds and the failure surfaces later as an opaque incident to whoever queries
first.

Only contract-derived sources are affected. Configured scalar projections come
from configuration rather than the catalog, so deploying a contract cannot
introduce one, and tokenized and exact text indexes are served by workers that
never had this coupling.

- `a_vector_field_deployed_into_a_running_daemon_becomes_queryable` deploys the
  vector contract into a running daemon, writes rows, and requires a projected
  query to become servable without the process restarting -- the sequence that
  today needs a restart, and which `benchmarks/perf-surface` already drives end
  to end.
- `a_source_whose_history_was_pruned_is_refused_at_deploy` prunes past what a
  fresh source needs, deploys a contract declaring one, and requires a typed
  refusal at the deploy rather than an accepted contract.
- `an_unregistered_source_answers_with_a_typed_refusal` asks the port for a
  source it does not hold and requires the typed refusal, so the internal-defect
  path cannot return by accident.

This record does not change what a cold source costs to activate, or when.
Demand activation, the population pass and the building result are ADR-0240 and
WP-777's, and admission hands off to them unchanged.

## Amendment 1 — OBL-0251-2 is unreachable, not unproven (Accepted 2026-09-21)

The maintainer accepted this amendment on 2026-09-21, on the action as
described rather than on this exact text.

Decision 2 requires that a source which cannot replay the history it needs be
refused when its contract is deployed. That is implemented, and the runtime
returns the refusal. What cannot happen is the deploy that would reach it.

WP-803 established, by driving the compiler and the compatibility comparison
rather than reading the policy, that a vector field cannot be added to an
existing contract at all:

- `VectorFieldDeclaration` has no nullability, so a vector field is always
  required and every create binding must initialise it.
- A production embedding is written by an `embed` effect needing three
  caller-supplied values, so initialising it needs three new required command
  inputs, each `RDB-K111` and `Incompatible`.
- A new command does not help, because the original command still creates the
  entity and now fails to initialise its new required field.
- The migration path does not relax any of this, which was measured rather than
  inferred from the shared compile call.

So OBL-0251-2's scenario requires a deploy that adds a columnar source to a
database that already holds history, and no such deploy compiles. The obligation
is discharged as unreachable, and its named proof is now the test that
establishes the unreachability.

This is a discharge, not a repeal. Decision 2 stands, and the guard it describes
becomes reachable again the moment a vector field can be added to an existing
contract — for instance if vector fields gain an optional form, or if the
command-evolution policy admits the inputs an embed effect needs. WP-803 records
that policy as unmapped.

`docs/performance/wp-803-live-field-addition.md` carries the evidence, and
`crates/riffdb-catalog/tests/live_field_addition.rs` the proofs.
