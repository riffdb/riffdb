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
optional field needs no initialisation, so it needs no command change, so
nothing touches the frozen command surface: the field itself is
`Compatible`, and the only friction is that the outcome shape changed,
which asks for an explicit successor version. That is the mildest class
above `Compatible` and entirely routine.

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

And the binding constraint is not about fields at all. It is that an
existing command's input list is frozen. Any change needing new caller
input is refused however ordinary the schema change behind it looks, which
is a sharper and more consequential statement than the one the package
started from.

## What this does not establish

**Whether the migration path admits a columnar source is moot.** A
migration cannot introduce a vector field, so it cannot bring a columnar
source into existence. Any source present after a migration was admitted at
the deploy that first declared it, which is the path ADR-0251 already
covers. The question the package opened with has an answer only because the
answer to the previous one closed it off.

**Whether the command-input constraint is intended at this strength.**
`RDB-K111` is `Incompatible` rather than `RequiresMigration`, so no
migration rescues it. Whether that is deliberate, or an artefact of
treating every command-surface change alike, is a question for whoever owns
the evolution policy rather than something this measurement answers.

**Anything about versioned or additive command surfaces.** A successor may
introduce a whole new command freely; only changing an existing one is
refused. Whether adding `CreateDocumentV2` beside `CreateDocument` is the
intended idiom for this situation has not been tested.
