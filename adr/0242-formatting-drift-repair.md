---
adr: "0242"
title: Formatting Drift Repair
status: accepted
tier: guarantee
date: 2026-09-19
accepted: 2026-09-19
acceptance: 'maintainer, in session, 2026-09-19: "i approve the text"
  (ADR-0242 as written)'
requires: []
amends: []
supersedes: []
requirements: []
packages: [WP-793]
obligations: []
review_triggers:
  - A formatting repair would be bundled into a change with another purpose,
    rather than landing on its own.
  - classify-change would be taught to read diff content rather than paths, to
    let a change of this shape compute as a lower tier.
---
# ADR-0242: Formatting Drift Repair

## Context

`cargo fmt --all -- --check` fails on main at `b8ee16934`. Four files carry
formatting drift: `crates/riffdb-commit/src/command_attempt.rs`,
`crates/riffdb-commit/src/command_validation.rs`,
`crates/riffdb-proto/src/durable.rs`, and `tests/unsafe_code_boundary.rs`.
The drift arrived with earlier merged work that was not re-formatted before
merge.

Because formatting is step two of every acceptance plan, **no change can pass
acceptance on main until this is repaired**. Any unrelated change is therefore
forced to carry the repair, and two of the four files are under
`crates/riffdb-commit/src/**`, which `classify-change` rules guarantee tier by
path. Carrying the repair silently raises an unrelated change's tier and
misrepresents what that change does.

The repair is inert. Ignoring whitespace and trailing commas, all four files are
byte-identical to their committed versions; the only token-level change is the
trailing commas rustfmt adds when it breaks arguments across lines, and almost
all of it is inside `#[cfg(test)]` code. Rust 1.97.0 and 1.98.1 rustfmt were
confirmed to produce byte-identical output on this tree, so the drift is not a
toolchain effect and this repair is not part of a toolchain change.

## Decision

1. Land the formatting repair on its own, touching nothing else, so the change
   that carries it is the change that is about it.
2. Accept the guarantee-tier ceremony this attracts. `classify-change` decides
   tier by path and cannot distinguish a rustfmt reflow from a change to commit
   semantics. That is the conservative direction for a guard to fail in, and it
   is not relaxed here to make a specific change cheaper to land.

## Options considered

**Fold the repair into the change that discovered it** — rejected. It would
have raised a surface-tier toolchain upgrade to guarantee tier, and the
resulting record would have described two unrelated things.

**Teach `classify-change` that a whitespace-and-comma-only diff is internal
tier** — not taken here. It is the durable fix and may be worth doing, but
relaxing a governance guard as a side effect of being blocked by it is the
wrong order. It needs its own record, its own acceptance, and an argument that
the content check cannot be fooled.

## Consequences

Formatting is verified before merge rather than repaired afterwards, so this
class of block does not recur silently. The underlying trap remains: the next
formatting drift in a guarantee-tier path will again demand guarantee ceremony
to repair. That is recorded as a review trigger rather than fixed here.
