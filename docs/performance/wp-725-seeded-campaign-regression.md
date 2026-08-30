# WP-725: why three seeded-campaign arms are red

Three arms fail on `main`:

- `campaign::per_merge_sweep_holds_the_oracle_and_reaches_the_swept_territory`
- `corpus::regression_corpus_replays_and_reproduces_its_territory`
- `subsumption::covered_rows_replay_as_pinned_campaign_schedules`

All three fail for one reason, and it is not flakiness.

## What fails

The pinned witness seed `0x51C2C147` for crash point
`command.commit.after-engine-commit` no longer resolves an interrupted commit
as **present**. Its report reads `in_flight_commit_present: 0` against
`in_flight_commit_absent: 6`.

The two outcomes mean opposite things (`campaign.rs`):

- **present** — the crash landed *after* the engine commit and before the
  acknowledgement; recovery must find the batch's complete effect graph.
- **absent** — the crash landed *before* the engine commit; the whole batch is
  re-attempted.

So every commit crash on that seed now lands before the durability fence, and
none after it.

## What did not fail

Atomicity. The campaign panics with "a PARTIAL batch survived the crash — a
genuine engine defect" whenever a recovered frontier is neither the
acknowledged nor the attempted frontier. Across 32 crashes and 32 recoveries on
the failing seed, it never fires. The oracle comparisons pass, the plan drives
to completion, and `in_flight_commit_absent` still resolves correctly.

This is a lost observation, not a broken guarantee.

## First bad commit

Bisected to `fa5d906c feat(storage): add clean-close fast startup`
(ADR-0156/ADR-0157). Its parent `c5c73858` passes; `fa5d906c` fails.

The first bisect run indicted the later merge `f991b19e`, because `fa5d906c`
was taken as the good boundary without being tested. It was not good. Testing
a boundary you assumed is worth the one extra run.

Neither ADR mentions the seeded campaign, so this consequence was not
anticipated by the records that introduced it.

## Why this matters more than three red tests

The sweep asserts `total.in_flight_commit_absent > 0` and **never** asserts
`total.in_flight_commit_present > 0`. Coverage of the after-commit case rests
entirely on the single targeted witness seed, which is exactly what stopped
producing it.

That is the more dangerous of the two cases. A commit that *is* durable must be
recognised as durable on recovery; failing to recognise it means re-applying an
already-committed batch. The campaign currently has no arm proving that case
still works, and it went quiet without anyone noticing because the assertion
that would have caught the coverage loss was never written.

## What must not be done

Re-pin the seed. Choosing a new seed that happens to produce **present** again
would turn the suite green while leaving the question unanswered: did the
after-commit window narrow, move, or close, and does clean-close fast startup
change when a committed batch becomes observable to recovery? A recovery test
is the last place to paper over a change in when durability is visible, and the
last place to do it is immediately before a release.

## The window is closed, not narrow

Swept 96 seeds from `SWEEP_SEED_BASE` under `COMMIT_PRESENT_ARMS_CONFIG` — the
configuration hand-tuned for this case, whose own comment records that the
physical window "is substantially rarer":

    wp725-sweep  seeds=96  completed=96  wedged=0  present_seeds=0
                 absent_total=296

Every campaign completed, none wedged, 296 interrupted commits resolved as
absent, and **not one resolved as present**. The reproduction is retained as
`wp725_commit_present_window_sweep`, ignored by default.

That settles the fixture-versus-engine question. A drifted schedule would leave
the window reachable from some nearby coordinate; a window no seed can hit in
96 attempts is closed. Re-pinning cannot work — and had someone tried, the
search would have failed slowly rather than revealing why.

## What WP-725 must still establish

1. What clean-close fast startup changed about the frontier a recovery
   observes, stated against `fa5d906c`'s diff rather than inferred.
3. A sweep-level assertion on `total.in_flight_commit_present > 0`, so this
   coverage cannot lapse silently again. Its absence is why a regression in the
   more dangerous recovery direction survived from 2026-08-26 to now.

## It is not an addressability problem either

The sim places crashes by store-operation ordinal, and clean-close fast startup
removed storage work from recovery -- notably an early return in
`journal.rs` that skips mutating an already-exact empty extent. Fewer
operations per recovery means the same ordinal budget covers more logical
progress, so the natural next hypothesis was that the injection simply steps
over a window that still exists.

It does not. Five crash-window shapes over 24 seeds each
(`wp725_commit_present_window_shape`):

| shape | crash_operations | max_crashes | present | absent |
|---|---|---:|---:|---:|
| narrow-early | (1, 48) | 32 | **0** | 2 |
| baseline | (1, 128) | 32 | **0** | 64 |
| wide | (1, 512) | 32 | **0** | 20 |
| late | (64, 512) | 32 | **0** | 16 |
| dense | (1, 128) | 64 | **0** | 110 |

Widening the window, shifting it later, and doubling crash density all produce
zero. With the 96-seed sweep that is 216 campaigns and not one after-commit
resolution.

## What the frontier is, and why that narrows it

The recovered application frontier is not a separately written pointer. It is
**derived** from the COMMITS table by
`command_authority_head_profiled(&commits, &events)` in
`read_commit_tail_profiled`. A commit record that is durable in COMMITS *is*
the frontier, in the same redb transaction that wrote it.

So there is no acknowledgement write that can lag a durable commit. If a batch's
record is durable, recovery observes it present by construction.

`fa5d906c` did not touch `command_authority.rs` or `retention.rs`, so that
derivation is unchanged across the regression. Whatever closed the window, it
was not the meaning of the frontier.

That leaves the window's width in *operations*. The simulator places crashes by
store-operation ordinal, so `in_flight_commit_present` requires at least one
observable operation between the durable commit and the end of the batch step.
If clean-close fast startup removed the post-commit work a crash used to land
in, the width is now zero and no ordinal can address it — which is the benign
reading, and would mean the guarantee is stronger rather than weaker.

## The question that was open, and its answer

Can a crash still land after a batch's engine commit and before its
acknowledgement, such that recovery observes the batch present?

**No, and not because the injection cannot reach it. The window has zero
width.**

The random draw could not prove that. `crash_operations` is a *range* the
schedule redraws from, so a zero across 216 campaigns is consistent with both
"the window is gone" and "the draw keeps stepping over a one-operation window".
`wp725_commit_present_window_ordinal_walk` removes the draw: `(k, k+1)` is
half-open, so `draw_crash_countdown` returns exactly `k`, and with
`max_crashes: 1` the run carries one crash at one known ordinal. Walking `k`
from zero until the plan exhausts covers **every reachable placement**.

    wp725-walk  seeds=4  ordinals<=768  present_placements=0
                absent_placements=823   seeds_exhausted_before_max=4

Four complete plans, every ordinal in each, 823 interrupted commits resolved
ABSENT, and not one resolved PRESENT. There is no store-operation ordinal at
which a crash leaves a batch durable but unacknowledged.

### Why that is the benign reading and not silent loss

Zero-width has two explanations that look identical from the count alone: the
commit fence is now the last fault-eligible operation of the batch step (benign),
or recovery is failing to observe a batch that really is durable (a durability
regression, and the dangerous one).

The oracle already separates them. `verify_model_against_inspection` checks
**every family in both directions** — admissions, outcomes, commits, entities,
index entries and epochs, provenance, events, outbox intents — so a store
holding effects the model lacks is a `DuplicateStoreEntry` or unmatched-store-
entry divergence, not a pass. It runs at the recovered frontier after every one
of those 823 crashes and diverged on none. The batches recorded ABSENT were
genuinely not durable.

The two remaining guards agree. The `PARTIAL batch survived the crash` panic
never fired, so no batch was half-applied; and `a clean shutdown loses nothing`
held at every plan exhaustion, so nothing acknowledged was later lost.

### Consequence

Clean-close fast startup made the batch acknowledgement **exact**: a
caller-visible commit failure now implies non-durability, where before there was
an in-doubt interval in which the caller saw an error and the batch was durable
anyway. That is a stronger guarantee, not a weaker one.

So `command.commit.after-engine-commit` asserts a state the engine can no longer
enter. Per the third reading named above, it must be **retired with this
reasoning recorded, not re-pinned** — a test asserting an unreachable state is
unfalsifiable rather than passing, and re-pinning it would manufacture a green
suite around a guarantee that changed.

Retiring it also means the sweep must gain the assertion whose absence let this
go unnoticed: the sweep asserted the ABSENT case and never asserted the PRESENT
one. It now asserts `total.in_flight_commit_present == 0` with the reasoning
attached, so a change that reopens the interval reds in the standard suite
rather than waiting for someone to run a diagnostic.

This is a behaviour change ADR-0156/ADR-0157 did not anticipate or record, and
it should be added to them as a consequence.

## The retirement was one of six changes, not the whole regression

Fixing the first failure and re-running produced a second, then a third. That
is the wrong way to read a suite whose failures share a cause, so
`wp725_corpus_territory_audit` replays every corpus entry and reports all of
them at once instead of stopping at the first. Its output is the actual shape:

| seed | before | after `fa5d906c` | treatment |
|---|---|---|---|
| `0x51C2C067` | rotated away 2026-08-11 | reaches its territory again | **restored** |
| `0x51C2C0DF` | rotated away 2026-08-18 | reaches its territory again | **restored** |
| `0x51C2C000` | active | torn decisions 23, below its pinned 30 | rotated to `0x51C2C307` |
| `0x51C2C147` | active | no interrupted admission resolves | rotated to `0x51C2C406` |
| `0x51C2C200` | active | no interrupted admission resolves | rotated to `0x51C2C504` |
| commit-PRESENT | witnessed by `0x51C2C147` | unreachable at every placement | **retired** |

So `fa5d906c` moved the physical operation stream for the third time in this
corpus's history, and it moved windows in *both* directions — invalidating three
witnesses while restoring two that an earlier layout change had rotated away.
Only the commit-PRESENT territory is gone rather than moved, and only it is
retired; the other five are the corpus's ordinary rotation and restoration
machinery doing its job.

That distinction is the whole point of separating the two receipts. A blanket
re-pin would have recorded six identical "the schedule moved" notes and buried
the one entry that is a behaviour change.

### What each treatment had to prove

Restorations were checked against the diff, not inferred: the full corpus replay
passes at `fa5d906c~1`, so neither restored entry reached its territory there.

Rotations were scouted by `wp725_scout_successor_witnesses` and held to the bar
the existing receipts record — a candidate must reproduce identical counters
across 12 reruns before pinning, or it is schedule luck rather than a witness.
All three successors met it.

The retirement carries the extra burden, because "no successor exists" is the
claim a re-pin cannot check. It is proved exhaustively over crash placement by
the walk, and it stays falsifiable: `0x51C2C406` keeps replaying with the
retired expectation **inverted**, so reproducing commit-PRESENT again fails the
corpus. `subsumption::retired_rows_replay_without_reaching_their_crash_point`
does the same for the SIM-006 row. Both were drift-checked by pointing the
retirement at a territory that is still live and confirming they red.
