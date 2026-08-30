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

## The open question, stated precisely

Can a crash still land after a batch's engine commit and before its
acknowledgement, such that recovery observes the batch present?

If clean-close fast startup narrowed that interval to the point where the
simulator's operation-counted crash injection can no longer address it, the
guarantee is intact and the test needs a different injection mechanism. If
instead a committed batch is no longer observable as present at recovery until
some later point, that is a change in when durability becomes visible, and it
belongs in ADR-0156/ADR-0157 rather than in a re-pinned fixture.

Nothing measured here distinguishes those two, and the difference matters more
than the red tests do.
