# WP-669 exact-current validated-prefix checkpoint reuse

> Historical prerequisite: ADR-0188/WP-774 later removed graceful shutdown as
> a checkpoint write point. The exact-current predicate is now consumed only by
> the bounded shutdown classifier; it retains matching bytes and never rebuilds
> a stale or ineligible checkpoint during close.

WP-669 activates a graceful-shutdown-only reuse path for an exact current
validated-prefix checkpoint. Startup validation still publishes one complete
proof for each process generation. At graceful shutdown, RiffDB still drains
and verifies the journal suffix first, then retains that startup proof
byte-for-byte when fixed durable identity, frontiers, retained state, and all
complete table counts remain exact. It performs no checkpoint proof rebuild,
entity-head snapshot replacement, metadata write, or durability fence in that
case.

Absence, V2 decode or self-hash failure, identity mismatch, frontier movement,
retention movement, retained-metadata movement, count uncertainty, or entity
snapshot count mismatch takes the unchanged full proof-building path. Reuse is
not an application-visible or selectable mode.

## Closed pre-change ledger

The ledger was captured before choosing the candidate on the full TicketDesk
dataset. Times are microseconds. The read-only shutdown suffix was already
empty; the stable cost was rebuilding the proof, replacing the complete
entity-head snapshot, and committing it durably.

| Stage | N1 | E2 |
| --- | ---: | ---: |
| Journal-suffix barrier | 2 | 2 |
| Retained-state read and outer bookkeeping | 201 | 231 |
| Proof construction | 124,313 | 91,266 |
| Encoding | 15 | 15 |
| Begin write | 29 | 14 |
| Entity-head snapshot replacement | 72,678 | 50,271 |
| Metadata publication | 68 | 59 |
| Durable commit | 20,367 | 23,379 |
| Inner checkpoint total | 217,477 | 165,015 |

The named inner stages close more than 99.99% of the inner checkpoint on both
hosts. The full changed-head seed boundary is intentionally different: its
outer suffix drain was 665,235 microseconds on N1 and 458,626 microseconds on
E2 before the ordinary 212,524/164,504-microsecond proof write. WP-669 always
executes that suffix barrier and never qualifies a changed authoritative head
for reuse.

## Candidate and rejected broader form

The first mechanics candidate admitted exact-current reuse at both startup and
graceful shutdown. It removed the duplicate proof work, but also changed when
startup released the independently scheduled columnar catch-up. That made
whole-process comparisons harder to interpret even though the checkpoint
stage itself remained small. The activated form is narrower:

- startup completes the ordinary structural and historical validation and
  publishes its one process-generation proof exactly as before;
- only graceful shutdown may reuse that proof;
- the journal suffix barrier precedes the exact-current decision;
- the decision decodes and self-hash-verifies the V2 checkpoint, then compares
  database and incarnation identity, registry digest, commit and audit heads,
  retention watermark, retained metadata, complete durable row counts, live
  entity count, live-plus-deleted chain-head count, and checkpoint snapshot
  row count;
- any uncertainty falls through to the existing builder and write transaction.

The proof is therefore paid once per process generation, not once per lifecycle
call. A runtime test pins byte-identical reuse, a second-startup test pins one
new chained proof per new process generation, corrupt-current and predecessor
fixtures pin fail-closed repair, and an architecture pin proves the exact path
returns before `begin_write` and the checkpoint commit hook.

## Cloud mechanics gate

Each host contributed five control and five final-candidate full-dataset runs.
Each run contains three read-only shutdown generations, for 15 checkpoint
observations per side. Two runs per side reversed candidate/control order.
Times are microseconds; p95 is the maximum of the 15 bounded observations.

| Host | Variant | Checkpoint median | Checkpoint p95 | Change |
| --- | --- | ---: | ---: | ---: |
| N1 | control | 222,752 | 233,582 | -- |
| N1 | final candidate | 5,371 | 11,378 | -95.1% |
| E2 | control | 160,270 | 177,642 | -- |
| E2 | final candidate | 3,829 | 5,654 | -96.8% |

Seed/startup generations continue through the full path. In the
order-balanced receipts their checkpoint stage was 856,893--949,524
microseconds on N1 and 550,055--613,244 microseconds on E2, confirming that the
candidate does not mistake a changed head for a reusable proof.

## Measured gate correction

The package initially proposed a 50% whole-graph shutdown p95 reduction. The
paired evidence falsified that metric as a candidate-specific gate before
closure. The columnar worker drains before the checkpoint begins and varied
independently from 0.4 to 900 milliseconds on N1 and from 0.4 to 628
milliseconds on E2. Reversing candidate/control order reversed which binary
appeared faster even though control checkpoint time stayed near 160--234
milliseconds and candidate checkpoint time stayed near 1--11 milliseconds.

Across the 15 observations, whole-graph p95 moved from 1,070,740 to 904,315
microseconds on N1 and from 409,811 to 638,894 microseconds on E2. Those values
track the pre-checkpoint columnar residue, not checkpoint behavior. WP-669
therefore reports the graph honestly but corrects its activation gate to the
isolated checkpoint p95 (at least 95% lower on both hosts), plus a public-load
no-regression gate. This does not waive a missed product metric or conceal a
candidate regression: WP-667 already owns and rejected bounded columnar
scheduling, and WP-668 introduced the exact ordered stage boundary that makes
the two owners distinguishable.

## Public mixed-load guard

Two paired E2 interactive c32 runs per side exercised the changed-head path.
All four runs completed with zero errors, conflicts, unavailable outcomes, or
idempotency mismatches.

| Metric | Control mean | Candidate mean | Change |
| --- | ---: | ---: | ---: |
| Full seed | 8.171 s | 8.088 s | -1.0% |
| Mixed throughput | 7,283 ops/s | 7,759 ops/s | +6.5% |
| Aggregate p50 | 2.032 ms | 1.966 ms | -3.2% |
| Aggregate p95 range | 18.87--19.92 ms | 17.83--18.87 ms | improved |
| Aggregate p99 range | 27.26--28.31 ms | 25.17 ms | improved |
| `create_comment` p50 range | 16.25--17.83 ms | 15.73--16.25 ms | improved |

The candidate's mixed-generation final checkpoints remained full writes, as
required. No public cell regressed by more than five percent.

## Compatibility and documentation impact

There is no durable-format, public protocol, contract-language, SDK, CLI, MCP,
configuration, or recovery-policy change. V1 checkpoints and corrupt V2
checkpoints remain ineligible for reuse and are replaced by the existing V2
full path. The handbook needs no public update because the optimization is an
internal graceful-lifecycle detail and does not change any supported operator
action or observable guarantee.

## Automated acceptance

- `the_shutdown_checkpoint_write_iterates_no_history_rows` proves the
  exact-current result is byte-identical and still O(1).
- `startup_republishes_once_and_shutdown_reuses_that_process_proof` proves one
  fresh proof is published for each validated process generation and only its
  same-generation shutdown reuses it.
- `an_uncertain_current_checkpoint_takes_the_full_repair_path` proves malformed
  current bytes cannot qualify and the ordinary path restores a checkpoint
  accepted by the next complete startup validation.
- Existing predecessor, wrong-identity, wrong-registry, wrong-frontier,
  wrong-count, wrong-fingerprint, retention, crash-before, crash-after, and
  finding-veto fixtures remain green.
- `an_exact_current_checkpoint_returns_before_opening_a_write_transaction`
  pins the production control-flow boundary ahead of `begin_write` and the
  durability hook.

Together these automated checks satisfy WP-669's `PERF-007`, `PERF-008`,
`PERF-018`, `REC-001`, `STO-001`, and `STO-002` obligations. The package and
app-baseline workspaces, policy and requirement-coverage checks, formatting,
and workspace Clippy all pass.

## Receipt custody

Value-bearing logs and JSON remain outside the repository under
`/home/user/tmp`.

- Pre-change N1 ledger JSON/log SHA-256:
  `e8dc86325a0288b6395ca0cdbb60c56fedfb31ca43cec1d4c29fe3823524c371` /
  `f4a7e49bb474e3358179f35edf03fa3505e7f317cc8de34afbbdc045ad30404b`
- Pre-change E2 ledger JSON/log SHA-256:
  `97ea367f62e61dd18c9c290bc76bb28ca3519dade45c6ee6de67a4c44968c601` /
  `0679104b19a87df09fb2080e0821123550052d52664fba2f6ddfd25dd608a66f`
- Normal-order N1 candidate JSON SHA-256 (rep 1/2/3):
  `18c619930227c0c39aeeea4d0c8994a7484af73f60156d55dd6bef9e6cf12cf6` /
  `b735118e82b712ba81da46bd6d64667c4323326eb217cea38e99b3ed192463c9` /
  `4d752e73de4f423d1725aadb3531d8417bb5b359315bed2e555ac5bfee4f58e7`
- Normal-order N1 control JSON SHA-256 (rep 1/2/3):
  `93d7eb27f68947deadcaaa730faf8eb9f452c4b5357f9dd5ab52fd121442cc22` /
  `15b2c895a8fc9db1e4333884518445a1401662e874625f8b736d0c4354f4802f` /
  `6675edd9aa1ff4c9cf284305e2de54feddef8e7804ebf5bb506073f503f87ac8`
- Normal-order E2 candidate JSON SHA-256 (rep 1/2/3):
  `99fcc46510d6ba30cfeff8ae6d0d455e782cca90d6c947a018c9c746b4db8253` /
  `0cb691679f26514f99748d552cb8f9c673dd5d90c25adf2bd4c5d6220b9365c3` /
  `ed3fba71530d7e7c9329cbe116b5ac34bca1b7aec274642e800cf4cae3332592`
- Normal-order E2 control JSON SHA-256 (rep 1/2/3):
  `80a18a28c2d279c15ea961f4ff4ee890a2b3c4e67d0d706de65e2529a7b378dc` /
  `a645068300e112f38fb3d217cfb7821fcf0263896f0aac5aa8015307335cbf07` /
  `09b52042e2333560ba53e5c7a3201b57bf12ea95b265e468c07d4c2594467f75`
- Order-balanced N1 candidate JSON SHA-256 (rep 1/2):
  `e9e726b5690fff319c529258b675bf645e0b828be2a51b7e55b0d093b1cfe854` /
  `e0b7123b4ed861d26535f11873fe8085bc32e4cdb33dbb04cf6416c3e47ca1ed`
- Order-balanced N1 control JSON SHA-256 (rep 1/2):
  `2c04f6e1ae2e49ecfd4d36fcf06a307a9aa76ce5b6904a35f14d9a9cd719ae2e` /
  `b00776e50625f4dc8289cc53b15c7a9ee3de808fa607db27ca2e4a0591d73139`
- Order-balanced E2 candidate JSON SHA-256 (rep 1/2):
  `739a96fd9dcb6aa19063bdf27e20fbe54c4374673a5e2dd0b20994252a2e92c4` /
  `100fe89708ce5dbf01508d98a15f0c6bf8438c790ba243c648be46addb3db33c`
- Order-balanced E2 control JSON SHA-256 (rep 1/2):
  `7c53c5b753e6cee39367f830a3ed59758c7d891021d054ff3dc9c4914cad07ae` /
  `bb0091230454c67dcd131166a3444379e978db71e3bcc52e79071ab40fdd3a45`
- E2 mixed candidate JSON SHA-256 (rep 1/2):
  `89217e92a56b24b11dd7c422cee5498ee7f6a40bb73acc8af2062f768b0b6e88` /
  `103d28a60663b53e7cbc223b31379acc95f877b4af70844a28b4c839ff9f77df`
- E2 mixed control JSON SHA-256 (rep 1/2):
  `8aa11f8b8508ff51a5965d29be74f298f896dc4c4c2670f941f1580354b7fe47` /
  `7761688a7d68227789b798771f4c454b4edfe7e96c9dc83d1694f0adcd4ce5ac`

The final candidate server binary hashes were
`649b0a69d1541352e6f1e4076b802f0604559da7bb33a6942ede06e2d909d2fc`
on N1 and
`e098ceb95e1cbc149a537c660db77a4c9ee815c1ee670af774b4a24d4c24fff9`
on E2. The common control server hash was
`61063a6eaef62c2e6c1d089dc2955c09a2563b55cebe7dbcf99f006caad4b7d6`.
