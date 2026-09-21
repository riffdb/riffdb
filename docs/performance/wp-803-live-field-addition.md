# WP-803: what adding a field to a live application actually does

Established by driving the compiler and the compatibility comparison, not
by reading ADR-0066. The three cases below are pinned by
`crates/riffdb-contract-compiler/tests/live_field_addition.rs`.

## The answer

**A field can be added to an application that already holds data.** It
classifies `RequiresMigration`, which is a supported path. What cannot be
added is a *command input*, and that is what refuses most attempts.

| successor | findings | overall |
|---|---|---|
| adds a field, set from an input the command already had | `RDB-K030` on the field, `RequiresMigration` | **RequiresMigration** |
| adds a field and the input to initialise it | `RDB-K030` on the field, plus `RDB-K111` on the input, `Incompatible` | **Incompatible** |
| adds a vector field | does not compile | -- |

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

So the chain closes: a vector field needs caller input, new caller input is
incompatible, and therefore **a vector field cannot be added to an existing
contract by ordinary evolution**. The refusal arrives as `InvalidCreation`
from the compiler, before the compatibility policy is consulted at all.

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

**Whether the migration path admits a columnar source.** ADR-0251 registers
one at deploy, and a migration is not a deploy. Untested, and the third
deliverable of this package.

**Whether the command-input constraint is intended at this strength.**
`RDB-K111` is `Incompatible` rather than `RequiresMigration`, so no
migration rescues it. Whether that is deliberate, or an artefact of
treating every command-surface change alike, is a question for whoever owns
the evolution policy rather than something this measurement answers.

**Anything about versioned or additive command surfaces.** A successor may
introduce a whole new command freely; only changing an existing one is
refused. Whether adding `CreateDocumentV2` beside `CreateDocument` is the
intended idiom for this situation has not been tested.
