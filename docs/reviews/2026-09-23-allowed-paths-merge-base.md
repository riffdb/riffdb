# check-allowed-paths and the merge base

Recorded finding for WP-808's fourth deliverable, 2026-09-23. The question:
`./scripts/check-allowed-paths` reads a package's `allowed_paths` from
`work_packages.yaml` at the base commit of the range, not from the working
tree, "so a change cannot widen its own scope in the same change." A package
that widens its own scope on a branch therefore cannot pass its own check
until the widening reaches the merge base — which cost the 2026-09-21 WP-807
session a cherry-pick of its widening commits onto main and a back-merge of
main into the branch, for no safety. Is reading at the merge base correct?

## What the check actually does

The range on a branch is `merge-base(HEAD, main)..HEAD`; on main it is
`HEAD~1..HEAD`. The check reads `allowed_paths` at the range's base commit.
If the working-tree `allowed_paths` differ from the base's, the check reports
`self_edit` — listing the added and removed globs — and **fails regardless of
whether any file is out of scope** (`ok = offenders.empty? && !self_edit`).
A package absent at the base falls back to working-tree paths, so a new
package is never blocked by this rule.

Reproduced on a scratch branch from main: one commit that adds a single glob
to WP-808's `allowed_paths`, then `check-allowed-paths --wp WP-808`, fails
with

```text
WP-808 edits its own allowed_paths in this change:
  + benchmarks/**
Widen a package's paths in a separate commit that precedes the package change; ...
```

even though that commit touches only `work_packages.yaml`, which is always
allowed. The failure is not about the touched files; it is the self-edit rule
itself.

## Why the WP-807 session paid the cherry-pick

WP-807 needed three successive widenings (`2ad7a089f`, `2b97fc67a`,
`fbdc1fc85`) on its branch. Because the branch's merge base with main sat
before those commits, every `acceptance --wp WP-807` run on the branch saw
the widening inside its range and failed on `self_edit`. The only way to make
the branch's own check pass was to get the widening *onto main* first — hence
byte-identical cherry-picks of the widening commits onto main
(`87ec80ed6`, `46c40a493`; `git range-diff` confirms `1: 2ad7a089f = 1:
87ec80ed6`), then back-merges of main into the branch (`e09de159f`,
`aa0de36de`) so the merge base moved past the widening. Two round trips
through main, per widening, to satisfy a check whose purpose is scope
discipline, not merge topology.

## The finding

**Reading at the merge base is the wrong base; reading at the widening commit
is the right rule, and the check already half-implements it.** The safety
property the rule exists for is: *a package's change must not be judged by
scope the same change invented.* That property does not require the scope to
predate the branch point; it requires the scope to predate the package's
*substantive* change. A widening commit that touches only
`work_packages.yaml` (a consequence path, always allowed) and does nothing
else cannot smuggle scope: it is itself the reviewed act of widening, it is
the only file it touches, and the package change that follows is then judged
against the widened scope exactly as intended. The WP-807 cherry-picks
satisfied no safety property that "the widening commit precedes the package
change *on the same branch*" would not have satisfied.

The check's own error message already states the correct rule — "widen a
package's paths in a separate commit that precedes the package change" — and
the separate-commit discipline is what every session actually follows. The
defect is only that "precedes" is implemented as "predates the merge base"
rather than "is an earlier commit in the range that touches only scope
declaration." The distinction matters exclusively on unmerged branches, which
is precisely where the cost landed.

**The repair, if taken, is one rule:** treat a commit in the range that (a)
touches only `work_packages.yaml` and (b) only widens (never narrows) a
package's `allowed_paths` as the scope-establishing commit, and read
`allowed_paths` from the last such commit rather than the range base.
Narrowing keeps failing as `self_edit`, because a package shrinking its own
scope mid-range is the shape worth catching. A package that both widens and
narrows in one commit keeps failing. No approval is added or removed: the
widening commit remains separate, visible, and governance-tier by the
manifest's own classification.

This is recorded as a finding and deliberately not implemented here. The
change would alter what a governance check accepts on every open package's
branch, it belongs to the check's own maintainers rather than a package whose
scope is documentation, and the cost it removes — cherry-picks that are
annoying but not unsafe — does not justify rushing it. The exit gate is
unaffected: this page is the recorded finding the deliverable names.

## Cost of the current rule, measured

Seventeen `widen allowed_paths` commits exist in history. Of the ten
sampled, nine sit on merged side branches (not on main's first-parent line),
which is the shape that pays the round trip. The WP-807 session alone paid
two cherry-picks and two back-merges for three widenings. The WP-808 session
paid one more (`7b92ea8da`, on the branch, merged by `e248681e9`) and would
have paid the same round trip had its branch run the check against its own
widening rather than landing the merge first.
