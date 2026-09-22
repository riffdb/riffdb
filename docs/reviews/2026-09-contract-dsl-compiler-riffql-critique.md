# Design review: RiffDB contract DSL, compiler, and RiffQL

Scope: `SPEC.md` §7 (contract language), §8 (compiler), §4.8 (migration), §24.5.3
(bounded collection mutations), §23 (demo contract); `adr/0002`, `adr/0013`,
`adr/0015`, `adr/0031`, `adr/0051`, `adr/0055`; plus the shipped grammar
reference (`docs/contracts/LANGUAGE.md`) and handbook pages that define the
actual authoring surface.

Method: read the normative text and the generated grammar, then tested each
claim against the demo contract (§23.1), the checked evaluation corpora
(`docs/evaluations/wp-722/runs/*/contract.riff`), and the published alpha
evidence. Severities are my judgement of consequence for the stated product
thesis ("data layer for the average multi-tenant SaaS application",
`docs/VISION.md:3`), not of implementation quality.

---

## F-01 — SPEC §7.3 is not the language. The normative surface is 20 grammar
versions stale, and §7.4 forbids exactly what ships.

**Severity: High (authority defect).**

`SPEC.md:2473-2546` enumerates the supported declarations: entities,
aggregates, commands, queries, deferred state machines, durable events,
projections. `SPEC.md:2565` then states the rule explicitly:

> Section 7.2 is the canonical v0.1 surface grammar. … A future syntax extension
> requires an accepted language/IR ADR and compatibility fixtures;
> implementations MUST NOT accept alternate spellings merely because they appear
> in an old example.

The shipped parser is grammar version 20 (`docs/contracts/LANGUAGE.md:5`) and its
top-level declaration set is:

```text
declaration = entity | event | enum | aggregate | command | workflow
            | projection | principal fact | row policy
```
(`docs/contracts/LANGUAGE.md:34-53`)

None of `workflow` (`:138-179`), `row policy` (`:60-77`), `principal fact`
(`:55-58`), `vector_field` (`:216-232`), `text_index` (`:233-267`),
`pattern_index` (`:268-282`), `delete_policy` (`:283-293`), `unique`
(`:206-207`), `reference` (`:208-215`), the `secret` field modifier
(`:195-200`), `service` values, `init_or_mutate`, `delete`, `bulk command`
(`for … in …`), `observe_or_initialize … decide`, or `reimport command` appears
in §7.2, §7.3, or the §7.5 grammar excerpt (`SPEC.md:2569-2584`).

**Consequence.** The document that `AGENTS.md` declares authoritative for the
contract language describes a language that has not been the language for
roughly twenty revisions. Two concrete harms:

1. An agent or engineer who reads §7 to learn the surface builds a wrong model
   and will not discover `workflow`, `unique`, `reference`, or `delete_policy`
   at all — the constructs that make the DSL usable for real domains.
2. §7.4's `DSL-001`…`DSL-012` restriction table (`SPEC.md:2550-2563`) is
   presented as the POC semantic boundary while `BLK-*` (`SPEC.md:8803-9229`),
   `WF-*` (`SPEC.md:9782-9828`), and `DEL-*` (`SPEC.md:9171-9210`) have
   superseded or extended it. `DSL-001` says command bodies "MUST NOT contain
   loops"; `BLK-002` (`SPEC.md:8809-8812`) permits exactly one bounded list
   expansion. Both are normative, and they disagree.

This is not a documentation nit. The repo's own change protocol says the
specification and accepted ADRs are authoritative and that conflicts require
human review rather than picking the easier reading. §7 vs. grammar v20 is such
a conflict, and it is unresolved.

---

## F-02 — The expressiveness wall is real and lands on ordinary SaaS shapes:
no write-influencing range read, no cross-aggregate invariant, no
command-readable derived state.

**Severity: High.**

Four independent restrictions compose into a hard ceiling.

**(a) Commands read by complete key only.** A command binding is
`read|mutate|create|init_or_mutate|delete` over an entity with an argument list
(`docs/contracts/LANGUAGE.md`, `EntityBinding`), and `SPEC.md:7738-7740` states:

> Write-influencing indexed range reads are rejected by grammar v1. The frozen
> prefix-epoch storage capability does not enable them; any future bounded IR
> requires accepted static target derivation and explicit up-front exclusion.

Restated at `SPEC.md:3384-3388`. So a command cannot ask "which row is first
available?" — it can only be told which row to touch.

**(b) Invariants cannot span aggregates.** `SPEC.md:2487`: invariants "may span
entities inside the same supported aggregate boundary." ADR-0170
(`adr/0170-partition-local-cross-aggregate-writes.md:100-113`) now permits a
command to *write* two aggregates in one partition, but that does not create a
declarable cross-aggregate invariant. A cross-aggregate rule can only be a
`require` in one command, which means every *other* command that mutates either
aggregate can violate it. "Sum of open invoices ≤ credit limit" is therefore
enforceable only by convention.

**(c) Commands cannot read projections.** `SPEC.md:2512-2519` lists projection
lookup as a *query* capability; command bindings are entity-only. Projections
are event-derived and `frontier transactionally_ordered` (`SPEC.md:2468`), i.e.
not in the command's transaction. So no command can enforce an invariant over
derived state, and no command can read a materialized aggregate.

**(d) No derived or computed fields.** Fields are stored values; there is no
`derived`/`computed` declaration anywhere in grammar v20. Conditional state
exists only as ADR-0163's single closed decision over one deferred initialized
binding (`adr/0163-compiler-sealed-command-decisions.md:60-90`), explicitly not
general branching.

**Consequence.** Judged against the §23 demo contract, the language is fine: the
demo is one entity, one aggregate, one conflict key, one projection, and two
commands that read by complete key. Judged against the checked evaluation
corpora, it is also fine — `docs/evaluations/wp-722/runs/library/contract.riff`
is 49 lines with two entities and one command. But those corpora were *chosen*
to fit. The shapes that break are the ones that make SaaS billing, project
management, and CRM non-trivial:

- **Slot/seat/queue allocation** ("assign the first free seat") — blocked by (a).
- **Credit limits, budget rollups, org-wide quotas** — blocked by (b) and (c).
- **Denormalized counters, computed status, aging buckets** — blocked by (c)
  and (d); the only route is a projection, which commands cannot consult.
- **Multi-entity workflows with compensation** — see F-03.

**What apps do when they hit the wall.** The documented answer is
`docs/contracts/COMMAND-INVARIANT-COOKBOOK.md:104-118`:

> An unavailable pattern is not an invitation to work around the compiler. Model
> the invariant, relationship, aggregate, command, or declared outcome so RiffDB
> can prove it.

That is a real answer for (b) — widen the aggregate — and it is honest. It is
not an answer for (a) or (c). For those, the only remaining move is to push the
decision into application code: read a page with RiffQL, pick a slot, then call
a command with that slot as an input. That is precisely the "client-side
check-then-write integrity" pattern the cookbook lists as intentionally
unavailable (`:109`), reintroduced one layer up, where RiffDB cannot see it and
ADR-0055's safety claim explicitly does not reach (`adr/0055:214-220`).

---

## F-03 — R-01's mitigation, "durable workflows outside commands," is a
promissory note with no owner.

**Severity: High.**

`SPEC.md:7632` is the entire mitigation for the highest-impact DSL risk:

> `R-01` | Contract DSL becomes a general programming language | Medium | High |
> … | Keep bounded expression language; require new constructs to preserve static
> dependency analysis; **use durable workflows outside commands.**

I searched the repository: that phrase occurs exactly once, in that table row.
There is no section, ADR, requirement family, or work package that specifies a
durable workflow facility outside commands.

What exists is something different:

- `workflow` / `transition` / `lease` declarations (`adr/0109`,
  `docs/contracts/WORKFLOWS.md:25-38`) are **inside** commands. They are a
  fixed-shape optimistic-concurrency and fencing primitive, not orchestration.
- `riffdb-scheduler` is an application worker, and `docs/contracts/SCHEDULERS.md:9-13`
  says plainly: "There is not yet a standalone scheduler daemon." Its one worker
  turn (`:34-52`) is a six-step client-side protocol the *application* must
  implement.
- Cross-command orchestration is explicitly post-MVP: `SPEC.md:7617` lists
  "Explicit `DistributedTx` and saga semantics" under §20.8 Post-MVP research
  tracks.

**Consequence.** R-01's mitigation is the load-bearing escape valve for F-02.
When the DSL cannot express a business action, the design says: express it as a
durable workflow outside commands. That facility does not exist, is not
specified, and is not scheduled. The risk register therefore records a
mitigation that cannot be executed, which is worse than recording the risk
alone — it makes the expressiveness ceiling look bounded when it is not.

Note the asymmetry: the *safety* side of this boundary is specified to an
extraordinary degree (idempotency identity, uncertainty recovery, fencing
tokens, at-least-once delivery, checkpoint ordering). The *orchestration* side
that would make those guarantees composable across commands is left to the
application, with no compiler, no declared outcomes, and no provenance.

---

## F-04 — Rejecting write-influencing range reads is more limiting than the
spec admits, and the workaround is unsafe by the project's own definition.

**Severity: High.**

Covered partly under F-02(a); it deserves its own finding because the
consequence is specific and the spec's framing understates it.

`SPEC.md:7738-7740` and `SPEC.md:3384-3388` reject the construct. The stated
reason is sound: a range read that influences a write needs a bounded
generation proof and up-front exclusion, and `PERF-012` (`SPEC.md:8398-8405`)
requires the compiler to reject a write-influencing range whose bounded
generation cannot be proved. That is a defensible engineering position.

The problem is what it costs. "Allocate from the first available slot" is not an
exotic shape; it is the core of seat assignment, inventory reservation, job
dispatch, connection pooling, and ticket assignment. Under this rule the
application must:

1. run a bounded RiffQL query to find candidates,
2. choose one in application code,
3. submit a command naming that exact key,
4. handle the conflict when another writer took it first.

Step 4 is where it breaks. The command's declared outcomes are the contract's
outcomes; a lost race on a slot the application picked is not one of them unless
the author anticipated it. The application must either retry (reintroducing the
loop the DSL forbids, now unbounded and outside the compiler) or surface a
generic failure. `adr/0055:214-220` concedes RiffDB "cannot infer that a
sequence of separately named commands was intended to be one business action."

**Consequence.** The restriction does not remove the unsafe pattern; it relocates
it from a place the compiler can prove things about to a place it cannot. The
honest framing would be: "RiffDB cannot express allocation-style selection; the
application must implement it, and RiffDB's guarantees do not cover the
resulting race." The current framing — a one-line bullet in §22.2 — reads as a
scope decision rather than a capability gap with a safety consequence.

---

## F-05 — Migration supports breaking changes only as stop-the-world offline
drains, and the "semantic replay and diff" MVP branching story has no design.

**Severity: Medium-High.**

§4.8 (`SPEC.md:937-1071`) and ADR-0076 through ADR-0081 define a genuinely
careful migration: `RequiresMigration` as a fourth compatibility class
(`SPEC.md:950-954`), row-local bounded transforms (`MIG-004`,
`SPEC.md:967-972`), lossless-or-guarded conversion (`MIG-005`), a dedicated
`MigrateContract` capability (`MIG-006`), exclusive drain of exactly one
database (`MIG-007`), immutable pre-migration backup plus staged database plus
atomic publication (`MIG-010`), and byte-exact preservation of history
(`MIG-011`). The engineering is sound and the invariants are the right ones.

Three limits matter for the product thesis:

**(a) Every breaking change is a full offline drain.** `MIG-007`
(`SPEC.md:987-990`): "The selected database MUST admit no application, worker,
subscription, or ordinary administration operation during the offline
interval." ADR-0076 rejects online dual-schema migration for the first release
(`adr/0076:95-97`). For a multi-tenant SaaS, a rename or a conflict-key change
is therefore a maintenance window. That is a legitimate POC posture, but it is
not stated as a product limitation anywhere in `docs/known-limitations.md`,
which discusses migration only in terms of Gate C key swaps
(`docs/known-limitations.md:204-211`).

**(b) No chaining, 32 parents.** `MIG-003` (`SPEC.md:956-960`): a server "MUST
NOT infer or automatically chain intermediate migrations," and at most 32
parent-specific artifacts are pinned. An application that ships 33 versions
without retaining every parent bundle loses the upgrade path from the oldest.
This is a real operational cliff and it is not surfaced as a limitation.

**(c) The MVP branching story is an open research question.** `SPEC.md:218`
lists Branching as "Not required" for POC and "Contract and representative-data
branches; semantic replay and diff" for MVP. `SPEC.md:7600` repeats it as an MVP
capability. But `SPEC.md:7785` lists it under §22.3 Open research questions:

> What is the smallest branch representation that enables agent replay and
> semantic migration review without promising transactional-history merge?

There is no ADR, no requirement ID, and no work package for branching. The MVP
capability list asserts a feature whose design is an open question.

**Consequence.** The migration story is credible *for what it does* — additive
and structural evolution with exact history preservation. It is not credible as
the answer to "what happens when a contract needs a breaking change," because
the answer is: full downtime, no chaining, and a branch/replay review workflow
that does not exist. Combined with F-02, an application that models its domain
wrongly at v1 faces a drain to fix it, with no rehearsal path.

---

## F-06 — The partition model leaks into application design as a silent,
author-time, largely irreversible decision.

**Severity: High.**

ADR-0051 is explicit that locality is not a deployment detail
(`adr/0051:93-96`):

> Every authoritative query must prove one partition route from parameters or an
> earlier binding. Every joined binding must prove equality with that same
> partition identity under the selected contract. **Single-node deployment does
> not relax this rule.**

The handbook then tells the author that this is a modelling decision with silent
failure modes (`docs/contracts/AUTHORING.md:89-108`):

> **a partition should be as wide as the widest thing you need to search or write
> atomically** … Two failure modes follow directly, and **both are silent**: a
> route that identifies a single row makes that entity's indexes useless … a
> unique key is enforced inside one partition.

And `adr/0055:190`: "Global uniqueness, nullable multi-row uniqueness variants,
deferrable constraints, and collation-dependent equality are outside this
milestone."

**Consequence.** Three things follow, and they cut against the "average SaaS"
pitch:

1. **The author must make a physical placement decision at contract-authoring
   time.** Get it wrong and the contract compiles and is quietly unusable
   (`AUTHORING.md:94`). The failure is not a diagnostic; it is a query that
   returns one row forever.
2. **Global uniqueness is unavailable.** "Unique email across the product"
   requires a fixed-scope-constant partition (`AUTHORING.md:110-113`), which
   collapses the whole collection into one partition — one locality unit, one
   index-scan scope, one placement unit. That is a scalability ceiling the
   author cannot see from the contract.
3. **Repartitioning is a Gate C migration** (`docs/contracts/MIGRATIONS.md:282-296`),
   i.e. an offline drain with `acknowledge repartition` and
   `acknowledge conflict` clauses. So the decision is expensive to reverse.

The design is internally consistent — this is what "bounded locality" costs. But
the pitch is "the data layer for the average multi-tenant SaaS application," and
the average SaaS author is not expected to reason about partition placement.
The handbook's own framing ("routinely confused", "quietly unusable") is an
admission that the abstraction leaks.

---

## F-07 — The agent-authoring claim is contradicted by the surface's own growth:
247 ADRs, 20 grammar versions, 40k lines of handbook.

**Severity: High.**

ADR-0057's product rule is that "Application authors own symbolic intent. The
compiler alone owns numeric, hash, plan, schema, and derived-authority
identities" (`adr/0057:38-46`), with bounded authoring diagnostics
(`adr/0057:95-105`) as the learning channel. `docs/VISION.md:45-46` states the
underlying thesis:

> a finite surface is a provable surface, and a provable surface is one an agent
> can be taught completely in context.

Measured against the repository:

| Surface | Size |
|---|---|
| Accepted ADRs | 247 files (`adr/`) |
| Grammar versions | 20 (`docs/contracts/LANGUAGE.md:5`) |
| Generated grammar reference | 1,235 lines (`docs/contracts/LANGUAGE.md`) |
| Handbook | 255 files, 40,667 lines (`docs/`) |
| Specification | 10,606 lines (`SPEC.md`) |

The evaluation harness itself concedes the point. `docs/evaluations/wp-722/README.md:16-18`:

> a diagnostic counts as sufficient only when its `help` field itself states a
> working source repair. Fix codes, handbook searches, and prior product
> knowledge do not rescue missing help.

That is a strong, well-designed gate — and it is a gate that only makes sense if
the language is *not* learnable from its grammar. And the sealed campaign
evidence is unambiguous: Terra campaign 01 scored 1, 2, 3, 2 with zero
evaluators completing their workload (`docs/agent-application-alpha.md:311-324`),
and the diagnosis was the authoring loop, not RiffQL execution (`:326-332`).

**Consequence.** The context burden is not a side effect of the design; it is
the design's central tension. Every accepted ADR that adds a bounded construct
(ADR-0107, 0147, 0153, 0163, 0170, 0173, 0175, 0176, 0177, …) makes the surface
strictly larger while the pitch requires it to be small enough to hold in
context. The mitigation — "learn it from diagnostics" — pushes the burden from
the grammar to the diagnostic registry, which is itself growing (the
`docs/getting-started/AUTHORING-DIAGNOSTICS.md` fix-code list, `RDB-C017`,
`RDB-C024`, `RDB-C025`, `RDB-QP002`, `RDB-QP003`, `RDB-QP007`, `RDB-QS009`,
`RDB-AR007`, …). A frozen grammar plus 247 ADRs is not learnable-by-agent; it is
learnable-by-agent-with-a-good-diagnostic-loop, which is a different and much
weaker claim than the one `docs/VISION.md` makes.

---

## F-08 — Version proliferation is the mechanism that keeps the language
inexpressive, and it is self-reinforcing.

**Severity: Medium.**

Every construct added since grammar v1 has required a new executable IR version
and byte-exact retention of all predecessors. Examples:

- `BLK-018` (`SPEC.md:8877-8881`): "Aggregate collection sizing MUST use
  least-sufficient successor grammar, executable-IR, and bundle identities …
  Older artifacts, hashes, locks, and bytes MUST remain readable and exact."
- `BLK-025` (`SPEC.md:8920-8928`): epoch-two reader retirement with two
  old-to-new descriptor-closure hash pairs, both old hashes "reserved and
  refused."
- ADR-0170 (`adr/0170:150-160`): cross-aggregate conflict ownership "does not fit
  the current encoding and requires V19."
- ADR-0177 (`SPEC.md:45`): high-cardinality collections select "grammar/
  executable IR/bundle V23."

**Consequence.** The marginal cost of adding expressiveness rises monotonically
with the number of constructs already added, because each addition must be
least-sufficient, must not disturb any existing plan hash, and must retain every
predecessor decoder. That is a rational compatibility discipline, and it is
working as designed. But it means the design has a built-in bias toward *not*
adding the general constructs (joins, derived state, range-influencing writes)
that F-02 identifies as missing — each would be the largest IR change yet. The
compatibility discipline and the expressiveness ceiling are the same mechanism
viewed from two sides, and no ADR acknowledges that trade.

---

## F-09 — The safety claim's boundary is exactly where the expressiveness wall
is, and the two are documented separately.

**Severity: Medium (framing and product risk).**

`adr/0055:207-220` states the supported claim and its limit:

> Through the stable application profile, RiffDB makes generic writes,
> unreviewed read shapes, raw storage-shaped page composition, violations of
> declared relationships and uniqueness, unbounded query work, and retry-unsafe
> command execution unavailable by construction.
>
> The claim is limited to declared contract semantics and RiffDB interactions.
> RiffDB cannot infer an undeclared business invariant, stop application code
> from calling an external service before or after a command, or infer that a
> sequence of separately named commands was intended to be one business action.

Read together with F-02, F-03, and F-04, the boundary of the guarantee and the
boundary of the language coincide almost exactly. Everything the DSL cannot
express — allocation selection, cross-aggregate invariants, derived-state
invariants, multi-command business actions — falls outside the claim. The
guarantee is real and well-evidenced; it is also narrower than "safe by
construction" reads to an application author, because the escape hatch is
application code and the claim does not follow it there.

**Consequence.** This is a positioning risk more than a technical one, but it
has a technical edge: the negative canary corpus
(`adr/0055:222-235`) tests that bad patterns fail *inside* RiffDB. It does not
test the composition of a bounded RiffQL read with an application-side decision
and a follow-up command — which is the pattern F-04 forces. A canary for that
composition would either pass (and show the claim's boundary) or fail (and show
a real gap).

---

## Sound areas (brief)

These are genuinely well-designed and I found no defect in them:

- **Deterministic compilation.** `CMP-020` (`SPEC.md:2819`) requires
  byte-identical bundles for identical source and compiler version, with no
  wall-clock in the bundle. Plan hash, bundle hash, and lineage materialization
  proof are separated cleanly (`SPEC.md:2825-2900`).
- **Explicit binding outcomes.** ADR-0015's mandatory `else` on every
  `read`/`mutate`/`create` binding (`adr/0015:26-43`) closes the implicit
  `NotFound` hole and keeps absence inside typed command semantics. The
  rejection of an optional `else` (`adr/0015:119-120`) is the right call.
- **Idempotency identity.** Excluding contract version from lookup identity so an
  uncertainty retry survives active-version deployment (`SPEC.md:2613`) is a
  subtle and correct decision.
- **Same-partition external reads.** ADR-0059's rule that an influential read
  outside the mutation aggregate is safe when it is a declared binding,
  captured as exact observation evidence, and revalidated in the commit
  transaction (`adr/0059:55-71`) is a real improvement over the prior
  organization-wide conflict domain, and it is backed by measurement.
- **Declared relationships and uniqueness.** Rejecting hidden target reads and
  requiring a dominating exact read with a declared missing-target outcome
  (`adr/0055:146-172`) makes referential integrity a compiler obligation rather
  than a convention.
- **Bounded dependent-key batches.** ADR-0054's seven static conditions
  (`adr/0054:38-54`) are a well-drawn line between a bounded junction traversal
  and a general join.
- **Migration history preservation.** `MIG-011` (`SPEC.md:1010-1019`) — no
  rewrite of commit, event, outcome, provenance, idempotency, or historical
  bundle bytes, with exactly one `EntityVersion` advance per changed entity — is
  the correct invariant and is unusually well specified.
- **Bounded partition-set queries.** ADR-0175's finite route set with one sealed
  local plan, global merge, and one opaque cursor
  (`docs/riffql/PARTITION-SET-QUERIES.md`) is a good answer to a real need
  without introducing a join.

---

## Summary

| ID | Finding | Severity |
|---|---|---|
| F-01 | SPEC §7.3/§7.4 describe grammar v1; shipped grammar is v20 | High |
| F-02 | No write-influencing range read, no cross-aggregate invariant, no command-readable derived state | High |
| F-03 | R-01's "durable workflows outside commands" mitigation is unspecified and unscheduled | High |
| F-04 | Range-read rejection relocates the unsafe pattern into application code | High |
| F-05 | Breaking changes require offline drain; MVP branching is an open research question | Medium-High |
| F-06 | Partition placement is a silent, author-time, expensive-to-reverse decision | High |
| F-07 | 247 ADRs / 20 grammar versions contradict the "learnable in context" thesis | High |
| F-08 | IR version proliferation is the mechanism that keeps the language inexpressive | Medium |
| F-09 | The safety claim's boundary coincides with the expressiveness wall | Medium |

The strongest single observation: **F-02, F-03, and F-04 are one problem.** The
DSL deliberately cannot express selection, cross-aggregate invariants, or
derived-state invariants; the documented mitigation for that is durable
workflows outside commands; that facility does not exist; and the actual
workaround is application-side check-then-write, which the project's own safety
claim excludes. The design is coherent and honest about each piece separately,
but no document connects them, and the risk register records a mitigation that
cannot be executed.

The second: **F-01 is a governance failure, not a documentation one.** The
repository's change protocol makes `SPEC.md` and accepted ADRs authoritative and
requires human review on conflict. §7 has been in conflict with the shipped
grammar for twenty revisions without resolution.
