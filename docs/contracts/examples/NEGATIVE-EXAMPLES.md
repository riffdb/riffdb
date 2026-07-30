# Negative examples and safe corrections

These are intentionally rejected application patterns. Diagnostic messages do
not echo source values; the code and span identify the symbolic defect.

## Mutation without idempotency

Removing `idempotency_key idempotency_key` from `AddArtifact` produces
`RDB-C010`. Restore one direct required `string<1..=128>` input and use it only
as the command idempotency declaration. Do not generate a new key on retry.

## Missing relationship proof

Removing the exact `read Collection(collection_id)` from `AddArtifact` while
creating an `Artifact` produces `RDB-C024`. Restore the dominating same-
partition target read and its declared missing-target outcome. Do not preflight
with a separate query.

## Unindexed page ordering

Changing `CollectionPage` to order artifacts by `title` produces `RDB-QP003`
because the contract has no matching bounded access path. Add a declared index
whose key supports the complete predicate and ordering, or keep the supported
`artifact_id` order. RiffDB will not fetch and sort the collection in client
code.

## Unbounded collection

Removing `take $artifact_limit` produces `RDB-QS009`. Restore an explicit
positive literal or typed `Limit` parameter. There is no unlimited application
collection.

## Role names an undeclared query

Adding `"SearchEverything"` to a role produces `RDB-AS007`. Declare and compile
the named query or remove it from the role. Do not substitute raw entity/scan
permissions.

## Editing generated output

Changing any file under `generated/` makes
`riffdb application lock --check` fail. Restore it with
`riffdb application generate --locked`; if symbolic intent changed, review and
write a new lock instead.
