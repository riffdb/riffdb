# Corpus receipts after bounded V2 startup

This repairs an integration-CI failure. It does not change WP-810's runtime
candidate, resolve its remaining latency question, or close either work package.

## Verified cause

The exact parent `e6755d37cc92635f992e4e184a956735bb1f11b7` passes the corpus
gate and every historical expectation. Its child,
`0d9edbce0593ec1227f66fd9af2ea934ee09ecf9`, enables accepted ADR-0260's bounded
V2 dirty startup. That path omits the complete-validation checkpoint write;
it changes recovery's physical operation stream while preserving the oracle's
authoritative-state requirements. Six historical crash coordinates move.

| Coordinate | Parent | Child/current | Predicate that changes |
| --- | --- | --- | --- |
| `0x51c2c067` | reaches territory | displaced | Torn decisions: 38 → 29; required minimum remains 30 |
| `0x51c2c200` | displaced | restored | Interrupted admission: 0 → 1 |
| `0x51c2c406` | displaced | restored | Interrupted admission: 0 → 4; commit-PRESENT remains retired |
| `0x51c2c504` | displaced | restored | Interrupted admission: 0 → 5 |
| `0x51c2c301` | displaced | restored | Torn decisions: 29 → 34; required minimum remains 30 |
| `0x51c2c41d` | displaced | restored | Interrupted commit-ABSENT: 0 → 8 |

Parent and child run copied, distinct test executables built with Rust 1.98.1.
The child's entire audit equals integration `ff7dd3036`, including every
retained campaign counter. Twelve further executions of the copied child
reproduce all 21 coordinates identically. Complete reports are retained for
20 coordinates; the historical engine-defect coordinate retains its existing
completed-outcome record. The parent and repaired integration pass the exact
corpus gate; the unrepaired integration fails it. The repaired gate uses the
full workspace's all-feature configuration.

The repair adds six causal commit references to the existing typed movement
fields. Every seed, generator version, predicate, numeric threshold, retirement,
original receipt, and successor chain remains. `0x51c2c067` keeps its forward
successor chain for the same four predicates; the 30-decision requirement is
not reduced. The original recovery oracle and final-frontier checks still run.
The repaired audit's campaign counters and predicate verdicts exactly equal
the unrepaired child's; only the stale receipt classifications change.

## Earlier exploration and limits

The initial whole-gate bisect found an earlier failure period beginning at
`0ef986b05`. Full audits showed subsequent movement at `bade83c21`, followed
by restoration of the original expectations before `e6755d37c`. The gate's
history is therefore **not monotone**. The initial bisect is retained as
exploration, not as attribution for today's six mismatches. Their attribution
uses the independently verified adjacent `e6755d37c` / `0d9edbce0` pair.

This corpus result removes the previously identified first full-CI blocker.
It does not claim that the remaining full battery has passed. Implementation
and fixture review, the remaining shared-delay resolution, and measurement
qualification still govern WP-810 completion.

## Reproduce

`summary.json` contains all parent, child, current and repaired audit rows;
binary and input digests; twelve-repeat evidence; and the earlier exploration.
Private logs and executables remain outside the public repository. Run against
the retained private directory layout:

```sh
python3 analyze.py /path/to/private/corpus-bisect /tmp/corpus-summary.json
python3 -m unittest -v test_analysis.py
```

The analyzer rejects changes to campaign execution, predicates or the seed
population, as well as a causal child that differs from the current failure.
