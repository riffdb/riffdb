# WP-803: what adding a field to a live application actually does

Established by driving the compiler and the compatibility comparison, not
by reading ADR-0066. Every case below is pinned by
`crates/riffdb-catalog/tests/live_field_addition.rs`, which sits beside the
catalog because deciding whether a successor may deploy is the catalog's
job: `validate_successor_compatibility` runs exactly this comparison and
refuses an `Incompatible` report.

## The answer

**A field can be added to an application that already holds data, and an
optional one is routine.** What cannot be added is a *command input*, and
that is what refuses most attempts.

| successor | findings | overall |
|---|---|---|
| adds an optional field, no command change | `RDB-K013` on the field, **`Compatible`**; `RDB-K021` on the outcome shape | **RequiresExplicitVersion** |
| adds a required field, set from an input the command already had | `RDB-K030` on the field, `RequiresMigration` | **RequiresMigration** |
| adds a required field and the input to initialise it | `RDB-K030` on the field, plus `RDB-K111` on the input, `Incompatible` | **Incompatible** |
| adds a vector field | does not compile, by deploy or by migration | -- |

The first row is the headline and it took three attempts to find. An
optional field needs no initialisation, so it needs no command change:
the field itself is `Compatible`, and the only friction is that the outcome
shape changed, which asks for an explicit successor version. That is the
mildest class above `Compatible` and entirely routine.

## The working idiom

A field you can add but never populate would not be much use, so the route
has to include something that writes it. It does:

> **Add the field as optional, leave every existing command alone, and add
> a new command that mutates it.**

Measured, that successor reports `RDB-K010` `Compatible` for the new
command, `RDB-K013` `Compatible` for the field, and `RDB-K021`
`RequiresExplicitVersion` for the existing command's outcome shape, which
changed because the entity it returns gained a field. Overall
`RequiresExplicitVersion`: deploy it against an explicit successor version
and it goes.

Nothing in that successor touches an existing command, which is why none of
the three refusal codes can fire. The writer must bind with `mutate` rather
than `read` -- a `read` binding is unwritable, and assigning through one
fails as `AssignmentThroughUnwritableBinding` before compatibility is
reached.

This does not extend to a vector field. A vector field cannot be optional,
so the create binding on every existing command still breaks, and no new
command changes that.

The second row is the important one. Two findings, and the overall class is
the more restrictive: the field is migratable and the **command surface
change is not**. Reading the overall class alone would say "you cannot add
a field", which is false and is what this package was opened believing.

## Why a vector field is worse than a hard case

A vector field never reaches compatibility comparison. The create binding
must definitely initialise every required non-key field, and a production
embedding is written by an `embed` effect that requires three
caller-supplied values: the vector, the model identity and the model
version. Supplying them means three new command inputs, each of which is
`RDB-K111`.

There is no optional form to escape into. `VectorFieldDeclaration` carries a
name, dimension, metric, source fields, staleness and optional production
and ANN clauses -- and no nullability, so a vector field is always required
and always needs initialising.

So the chain closes: a vector field cannot be optional, every create
binding must therefore initialise it, initialising needs caller input, and
new caller input is incompatible. **A vector field cannot be added to an
existing contract at all.** The refusal arrives as `InvalidCreation` from
the compiler, before the compatibility policy is consulted.

The migration path does not relax it. `compile_contract_migration_successor`
compiles the candidate through the same path with identity renames bound
first, and a rename cannot make a required field initialised. That is
measured rather than inferred from the shared call.

Nor does a new command. A successor may introduce a command freely, so
`CreateDocumentWithEmbedding` is not what refuses -- the **original**
command is. It still creates a `Document`, and that entity now has a
required vector field it does not initialise, so a binding that compiled
yesterday stops compiling today.

That last point is the general rule these cases keep circling, and it is
wider than vectors:

> **Adding a required field breaks every existing command that creates the
> entity.** It compiles only if every one of those bindings can set the new
> field from what it already has.

The migratable case earlier in this page passes precisely because it could:
`subtitle` was set from the `title` input the command already took. A vector
field can never be set that way, because its value must come from the
caller.

This is why WP-802's OBL-0251-2 could not be driven end to end. Reaching a
pruned-history refusal needs a database with history that does not yet
declare a columnar source, and then a deploy that adds one. The second half
is unreachable, so the obligation's scenario is narrower than ADR-0251
implies rather than merely untested.

## What this corrects

WP-803 was opened on the reading that ADR-0066's additive set omits fields
on existing entities, so "the most ordinary schema change in any database
is a migration here". The first half is right and the framing was wrong in
two ways.

Migration is not a consolation. `RequiresMigration` is a distinct class
between `RequiresExplicitVersion` and `Incompatible`, with a migration
design behind it (ADR-0076 through ADR-0078), and the field addition sits
squarely in it.

And the binding constraint is not about fields at all -- though an earlier
draft of this page got its shape wrong twice before landing on it.

That draft said an existing command's input list is frozen. **It is not.**
An optional input reports `RDB-K013`, `Compatible`, the same code an
optional entity field gets; only a *required* input is `RDB-K111`. An
application can ask its callers for something new, provided it can proceed
without it.

What refuses in that case is a third thing again: `RDB-K110`, an existing
plan change, because the command gained the instruction that uses the input.
Nor is the plan categorically frozen -- `instructions_compatible` admits
some added requirements under dependency rules this page has not mapped.
What is recorded here is what was measured, and the honest summary is
narrower than any of the three sentences that preceded it:

> A required field must be initialised by every existing command that
> creates the entity. Whether a given successor can arrange that is decided
> by a policy with at least three distinct refusal codes, and the overall
> class alone does not say which one fired.

## What this does not establish

**Whether the migration path admits a columnar source is moot.** A
migration cannot introduce a vector field, so it cannot bring a columnar
source into existence. Any source present after a migration was admitted at
the deploy that first declared it, which is the path ADR-0251 already
covers. The question the package opened with has an answer only because the
answer to the previous one closed it off.

**What the command evolution policy actually permits.** Three refusal
codes were seen -- `RDB-K111` for a required input, `RDB-K110` for a plan
change, `InvalidCreation` from the compiler before either -- and
`instructions_compatible` admits some added requirements this page did not
characterise. Mapping that policy is its own work, and every summary of it
attempted here has been wrong at least once.

**How wide the blast radius is on a real contract.** These are
single-entity contracts with one or two commands. An entity created by many
commands multiplies the work of adding a required field to it, and none of
that is measured here.
