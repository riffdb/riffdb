# ADR-0172: Case-Insensitive Text Matching

- **Status:** Accepted
- **Direction approved:** 2026-08-30
- **Exact text accepted:** Yes, 2026-08-30
- **Accepted:** 2026-08-30
- **Acceptance reference:** Maintainer accepted this record's exact text in the
  current Claude Code session, on the gap recorded under "Evidence"
- **Decision deadline:** Before an adapter ships case-insensitive matching by
  maintaining its own folded shadow field
- **Requires:** ADR-0131
- **Amends:** SPEC's operational text-index and exact-text provider vocabulary
- **Defines or blocks:** ADR-0173

## Context

`unicode_fold_v1` is declared, specified, bound-charged, and wire-encoded, but
does not execute. Nothing that needs case-insensitive matching can use it.

What already exists:

- `TextKeyProfileV1::UnicodeFold` in the schema IR, specified as "Unicode
  17.0.0 NFKC followed by full non-Turkic case folding".
- Durable index-encoding discriminant `3`, decodable today.
- `UNICODE_FOLD_V1_MAXIMUM_EXPANSION = 18`, already charged in
  `schema_lowering`, because folding is not length-preserving — `ß` becomes
  `ss` and ligatures expand.
- The compatibility rule that a profile change is durable index identity and
  goes through migration and rebuild.

What is missing is exactly two things, plus one that was never started:

- `operational_index.rs` refuses the transform with "Unicode-fold text-key
  profile is not linked". The fold implementation is absent.
- `operational_component_capabilities` reports every capability `false` for the
  profile, so no predicate can reach it even once the transform exists.
- `ExactTextProfileV1` has one variant, `BinaryUtf8V1`. The provider that
  serves `starts_with`, `ends_with`, and `contains` has no fold variant at all,
  so linking the index encoding alone would leave the provider case-sensitive.

## Evidence

An MLflow tracking-store adapter requires `ILIKE` with SQL semantics. Its
options today are a lowercased shadow field maintained by the adapter, which is
ASCII-correct and wrong for Turkish dotted-I and several Greek cases, or no
case-insensitive matching. `docs/contracts/AUTHORING.md` states the position
plainly: `binary_utf8_v1` "is case-sensitive and performs no Unicode
normalization. The `unicode_fold_v1` profile is reserved but is not executable
in the current build."

A shadow field is not a neutral workaround. It is a second durable field, a
second index, and an adapter-side transform that RiffDB cannot verify — and it
becomes dead weight the moment the profile links, at which point swapping to it
is an index identity change requiring a rebuild.

## Decision

Link `unicode_fold_v1` and make it reachable from both text surfaces.

**One transform, two consumers.** A single frozen implementation performs NFKC
followed by full non-Turkic case folding. The operational index encoding and
the exact-text provider call the same function; a value and a needle are folded
identically, or matching is not symmetric.

**The tables are pinned, not depended upon.** The fold data is vendored or
generated into the repository and pinned to Unicode 17.0.0. A floating
system-provided Unicode library is refused: a library upgrade would silently
change index contents, which the existing rule already forbids — "RiffDB never
silently changes a text profile during a software upgrade." A future Unicode
revision becomes `unicode_fold_v2`, a new profile with a new discriminant,
never an in-place change to v1.

**Provider profile parity.** `ExactTextProfileV1` gains `UnicodeFoldV1 = 2`,
and the provider checkpoint format advances, because the bytes it stores for
matching change. Existing `BinaryUtf8V1` checkpoints stay readable and are not
rewritten.

**The original value is retained.** The provider matches on folded bytes and
must not return them: folding is not reversible, and an application asking for
a title expects the author's title. Result hydration continues to come from the
canonical entity, not from the matching representation.

**Capabilities are enabled to exactly the binary profile's set** — equality,
membership, range and complement, prefix, and order — computed over folded
bytes. Folded comparison is still bytewise, so cursor safety and order
stability carry over unchanged.

### What this deliberately does not include

SQL `LIKE` wildcard vocabulary. `x%`, `%x`, `%x%`, and `x` map onto
`starts_with`, `ends_with`, `contains`, and `==`; interior `%` and `_` do not
map onto anything, and the obvious decomposition is unsound:

- `a%b` as `starts_with 'a' && ends_with 'b'` over-matches when the fragments
  overlap. `"ab"` satisfies both halves and does not match `ab%ab`.
- `a%b%c` loses the constraint that `b` falls between `a` and `c`.
- `_` constrains length and position and has no anchored equivalent at all.

Making those exact requires generating candidates from anchored fragments and
then verifying the full pattern against a bounded candidate set. That is a
residual filter with a compiler-declared budget — the same shape as the
dependent key batch — and it is a separate decision with its own cost model. It
is not smuggled in here, and until it exists an adapter must reject the
patterns it cannot express rather than approximate them.

## Options Considered

**Leave it reserved and let adapters fold.** Rejected. Every adapter that needs
case-insensitive matching reimplements NFKC and case folding, each slightly
differently, in a language RiffDB does not control, against a durable field
RiffDB cannot verify. The result is per-adapter correctness for a property the
engine already specified.

**Link a system Unicode library rather than pinning tables.** Rejected. It
makes index contents a function of the host's library version, which
contradicts the accepted rule that a software upgrade never changes a text
profile. The failure is silent and appears as missing search results after an
unrelated upgrade.

**Fold at query time instead of index time.** Rejected. Folding the needle but
not the stored value requires scanning every row to fold it for comparison,
which is unbounded work and defeats the index.

**Add the fold to the index encoding only.** Rejected as incomplete: it leaves
`contains` and `ends_with` case-sensitive, which is most of what `ILIKE` is
used for.

## Consequences

Case-insensitive matching becomes an engine property with one implementation,
one specification, and one migration path.

Cost is paid in index size. The 18× expansion bound is already charged by the
compiler, so a contract declaring the fold on a large string field may exceed
`RDB-C020` where the binary profile did not. That is honest arithmetic rather
than a regression, but it will surprise authors, and the diagnostic should say
which profile drove the charge.

Adopting the profile on an existing index is an identity change and requires
the rebuild path. Adapters currently maintaining folded shadow fields will want
to migrate; they should be told the profile is coming before they invest
further in the shadow.

## Compatibility

Additive. Discriminant `3` is already accepted by the index decoder, so no
durable format widens. `ExactTextProfileV1::UnicodeFoldV1 = 2` is a new
provider variant; existing checkpoints declare `BinaryUtf8V1` and are read
unchanged. No existing contract's behaviour, bytes, or plan hash changes,
because no existing contract can have compiled against a profile that refused
to lower.

## Security

None beyond existing text handling. Folding is a total function over valid
UTF-8 with a bounded expansion already charged; it introduces no new
caller-controlled work. The fold must be applied before any bound check, so an
adversarial needle cannot expand past a limit that was measured pre-fold.

## Standing Design Tests

- Folding a value and folding a needle use the same function; a value that
  matches case-insensitively matches under every case permutation of the
  needle.
- The expansion bound holds for the worst case in the pinned tables, and the
  compiler charges it.
- A contract declaring `unicode_fold_v1` produces a different index identity
  than the same contract declaring `binary_utf8_v1`.
- The pinned Unicode version is asserted by a test, so a table regeneration
  that changes the version fails rather than silently re-folding.
- Provider results hydrate the original value, never the folded bytes.

## Testing

A conformance corpus of fold pairs covering case, NFKC compatibility forms,
expanding folds (`ß`, `ﬁ`), and the non-Turkic dotted-I decision, asserted
against the pinned Unicode version rather than against the host.

## Requirements and Work Packages

Needs a work package covering the linked transform, both profile enums, the
capability table, the provider checkpoint version, and the conformance corpus.

## Decision Deadline

Before an adapter ships a folded shadow field. That investment is wasted work
that later needs an index rebuild to undo.
