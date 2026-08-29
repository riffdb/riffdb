# ADR-0168: Index Cover Evolution Through Migration

- **Status:** Accepted
- **Direction approved:** 2026-08-29
- **Exact text accepted:** Yes, 2026-08-29
- **Accepted:** 2026-08-29
- **Acceptance reference:** Maintainer exact-text acceptance in the current
  Claude Code session for drafted commit `a448af6d`
- **Decision deadline:** Before the next covering-index change is authored
  against the stated prohibition
- **Requires:** ADR-0124 and ADR-0153
- **Amends if accepted:** SPEC's prohibition on an existing index identity
  gaining or changing a cover
- **Defines or blocks:** Nothing; this records which of two disagreeing
  authorities is correct

## Context

`SPEC.md` states, of covering indexes:

> An existing index identity MUST NOT gain or change a cover; evolution
> declares a new index and uses the ordinary receipted rebuild/migration path.

Nothing enforces this. Any difference in an existing index collapses to one
compatibility code:

```rust
// crates/riffdb-contract-ir/src/compatibility.rs
Some(old) if *old != index => {
    add(findings, CompatibilityCode::KeyLayoutChange, index_path);
}
```

`KeyLayoutChange` is on the migration compiler's compatible-anyway list, so a
successor that alters a cover compiles, seals, and migrates. There is no
diagnostic code for a cover change at all.

The initial reading of this gap was that it left stale covers behind. It does
not. `IndexSchema` derives `PartialEq` and includes `cover_fields`, and the
rebuild derivation is:

```rust
if reindexed.contains(&entity.id())
    || old_indexes.get(&index.id()).is_none_or(|old| *old != index)
{
    ordered.push((.., MigrationStepKindV1::RebuildIndex { .. }));
}
```

A cover change therefore derives a `RebuildIndex` step and the rows are
rebuilt from the post-image with the correct cover. The behaviour is safe; it
is the prohibition that is stricter than safety requires.

That distinction matters because the adjacent defect fixed this week was real:
`riffdb-catalog` wrote a hardcoded empty cover during migration, so rebuilt
entries carried no covered values at all. That was a rebuild that ran and
produced wrong data, not a rebuild that failed to run. With it fixed, and with
a Gate-B arm asserting the rebuilt cover against the successor schema, the
receipted path is exactly the mechanism SPEC's own sentence points authors
toward.

## Proposed Decision

SPEC's prohibition is relaxed to match the implementation. A cover change on an
existing index identity is permitted and travels the receipted migration path,
which derives a rebuild and reconstructs every entry from the post-image.

The exact proposed replacement for the quoted sentence is:

> An existing index identity MAY gain or change a cover. The change is a
> migration-classified difference: it derives an index rebuild and every entry
> is reconstructed from the post-image, so a cover is never carried forward
> from a predecessor layout. Declaring a new index identity remains available
> and is preferred when readers must observe both covers during a rollout.

No compiler change accompanies this. The behaviour already conforms.

## Options Considered

**Enforce the prohibition.** Add a distinct `IndexCoverChange` compatibility
code and refuse it, forcing authors to declare a new index identity. This keeps
the stated invariant true and would have been the right call if the rebuild did
not happen. It was rejected on measurement: the rebuild does happen, is
asserted, and refusing it would reject a migration that is demonstrably safe
while offering the author no capability they do not already have.

**Leave the divergence and document it.** Rejected. An unenforced MUST in a
specification an agent is expected to read as authoritative is worse than
either enforcing it or removing it — it trains readers to treat normative text
as advisory.

## Consequences

One fewer prohibition to enforce, and one fewer place where SPEC and the
compiler disagree. Authors keep the option of a new index identity, which
remains the right choice during a rollout where both covers must be readable.

The general finding stands and is not addressed here: `KeyLayoutChange` is a
single code for every difference in an existing index, so a genuinely
dangerous index change and a benign one are indistinguishable in a
compatibility report. That is worth separating on its own evidence rather than
as a side effect of this record.

## Compatibility

None. No durable format, artifact, or compiled behaviour changes.

## Security

None. The rebuild already reconstructs covered values from the post-image, so
no cover can leak a predecessor's field set.

## Standing Design Tests

- A successor whose existing index gains a cover derives a `RebuildIndex` step
  for that index.
- The rebuilt entries carry the successor's complete cover field set, taken
  from the post-image rather than the pre-image.

## Testing

`the_rebuilt_covering_index_carries_the_complete_cover_from_the_post_image` in
the Gate-B migration arms, which fails with `left: [] right: [FieldId(4)]` when
the catalog cover encoding is reverted.

## Requirements and Work Packages

Amends SPEC's covering-index section. No work package.

## Decision Deadline

Before the next covering-index change is authored against the stated
prohibition.
