# WP-641 cold-start service-audit reliability

WP-641 closes the cold-start `AuditUnavailable` finding without changing the
service-audit or authoritative-readiness contract. The failure was a secondary
symptom of redb rejecting an index-free writer-private detached group as an
invariant violation. It was not a checkpoint activation race and was not
repaired by making audit optional, retrying audit, or acknowledging work before
its audit transition.

## Reproduction and classification

The retained exact WP-640 baseline and candidate both fail on a fresh E2
database before the first checkpoint threshold. Current runtime reproduction
then narrows the same boundary:

1. ten `CreateOrganization` calls reach the service;
2. the first published-frontier command unit commits while its durable fence is
   still unpublished;
3. the next command unit therefore selects the exact writer-private frontier;
4. detached staging requires a retained index-generation base even though
   `CreateOrganization` advances no index generation, returns
   `InvariantViolation`, and stops the coordinator with one unit incomplete;
   and
5. the service's mandatory terminal-failure audit observes the stopped
   coordinator and returns `AuditUnavailable`, which correctly removes
   authoritative readiness.

The current-runtime failure completed nine commands in one atomic group, then
stopped with ten service preparations, ten coordinator waits, and only nine
service finishes. It recorded one successful frontier-equivalence check, zero
frontier failures, zero preparation proof mismatches, and zero checkpoint
activation. Repeating with a different intake split completed three commands
and failed on the following seven-command private unit. Disabling only the
writer-private parallel detached branch made the same fresh-database workload
complete all 19,220 commands. Published-frontier parallel evaluation remained
enabled throughout the control.

Static branch diagnostics narrowed the stop to
`RedbEmptyBatch::stage_detached_group`. The detached reservation path retains
an index-generation base lazily, only when at least one write plan advances an
index. Staging incorrectly treated the therefore-valid `None` for a wholly
index-free group as missing proof. This classifies the primary defect as a redb
detached-staging invariant, the readiness loss as the required consequence of
that actor stop, and `AuditUnavailable` as the correct fail-closed secondary
result. No service-audit append, checkpoint, or journal fence was omitted.

## Repair

Detached staging now distinguishes the two closed cases. A retained generation
base remains mandatory whenever any detached write plan advances an index.
When every retained write plan has zero index-generation advances, staging
uses the already-captured exact generation map unchanged. The final equality
check still proves that applying the record graphs did not invent an index
advance. Unit tests cover both the valid index-free case and the invalid
missing-base-with-advance case. Writer-private and published-frontier parallel
preparation remain enabled.

The repair changes neither command ordering nor durable format. Successful
commands still commit mutation, outcome, event, provenance, commit record, and
their started/terminal audit transitions atomically. An internal failure still
stops authoritative readiness, and an audit outage is still returned rather
than hidden or retried around.

## Cloud matrix

The fixed binary was built natively on each four-vCPU cloud profile. Every
cell used the full 19,220-command TicketDesk seed over public gRPC at client
concurrency 128 and a distinct empty database. The first three cells started
from a cold harness sequence; the fourth repeated the fresh-database seed with
the host build and filesystem caches warm.

| profile | cold 1 | cold 2 | cold 3 | warm-host fresh DB | result |
|---|---:|---:|---:|---:|---|
| E2 `e2-highcpu-4` | pass | pass | pass | pass | no recurrence |
| N1 `n1-standard-4` | pass | pass | pass | pass | no recurrence |

For every cold cell, the weighted command-completion-group total is exactly
19,220, the authoritative entity inventory is exactly 19,220, the command
frontier-equivalence failure count is zero, and the process reaches its clean
shutdown evidence boundary. No harness retry is used to turn a failed cell
green.

## Receipts

The retained value-free logs and JSON reports remain on their inventoried
hosts under `/home/kevin/tmp`.

| evidence | profile | path | SHA-256 |
|---|---|---|---|
| exact WP-640 baseline failure | E2 | `wp640-52e-baseline-e2.log` | `9aa9f4db9c14b60ed2cdc242f0a72a66b2156c3507255b7a425d1a538f40c70c` |
| exact WP-640 candidate failure | E2 | `wp640-checkpoint-e2.log` | `cca711ccaaaf84f601d14dae093d3cffd066d6d34d1c4b2ef8f15efa37d95d48` |
| current-runtime failure | E2 | `wp641-cold-current-1/run.log` | `fc98f039b01647631f01f20371243737a220aa19fc602f2d1f474a93d388a5ba` |
| fixed cold 1 | E2 | `wp641-indexfix-release-1/report.json` | `e210044b3c63c7c8e4756320eee9b52ea2abcfd2cf156649643843f36f5efe0fd0` |
| fixed cold 2 | E2 | `wp641-indexfix-release-2/report.json` | `903f8bb46e21bb4ae21d3ba111d6d199420b301413eac0ed22ce4f5a3ce55885` |
| fixed cold 3 | E2 | `wp641-indexfix-release-3/report.json` | `317f280ba5ac90bc2911ad95b750331bea396a6712455feda4e6ad87099253b7` |
| fixed warm-host fresh DB | E2 | `wp641-indexfix-release-4/report.json` | `27bd854d36550b34f169439f47f514408d09539075e16bd04d1cafe9e08cf36e` |
| fixed cold 1 | N1 | `wp641-indexfix-release-1/report.json` | `f6ae2a7d5c337461cf06492fedbafdb78855d14074c606baa540ee1b74d04e8f` |
| fixed cold 2 | N1 | `wp641-indexfix-release-2/report.json` | `2d50191afb44ba7e140d0707c7911ea4f09c16081c313b843dca757c03f31c71` |
| fixed cold 3 | N1 | `wp641-indexfix-release-3/report.json` | `c971a56ad75a8fbba47ef55233b27b460dccf86b8c06e4b12a4a447cc2f748c8` |
| fixed warm-host fresh DB | N1 | `wp641-indexfix-release-4/report.json` | `4d213249e681fdb488801b691995bee349b74d3673de3831775cb97b33f1ac01` |

Requirement coverage: PERF-008, REC-001, REC-002, STO-001.
