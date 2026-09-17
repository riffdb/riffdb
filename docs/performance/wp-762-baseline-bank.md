# WP-762 baseline bank and freeze start

The ADR-0183 performance-package freeze starts from one qualified baseline
receipt set on both inventoried cloud profiles, banked under ADR-0171's
gated-backend stability rule. This page records what was banked, from which
exact artifacts, and the values the freeze and the WP-763 activation
arithmetic compare against. Nothing here is a standing performance promise;
every number is revision-specific evidence.

## Identity

| Field | Value |
|---|---|
| Source revision | `48bcb5ced552b7a19d6ac431f05432ae70deeaa2` |
| `riffdbd` SHA-256 | `52de06848ed2c243ceafefb29de1c7be4fd6b69dd0f8f84938d6238b40e7f783` |
| App-baseline runner SHA-256 | `fb9babf42923833244eb9f50651337aac299f480c57d36faa0f48513c9082204` |
| Harness (`benchmarks/run-app-baseline`) SHA-256 | `998f7d1f769bd2d7b75fda5eb28d9cd18f41049496df9541b8f19eba014e9070` |
| Stability rule | `method.stability_rule_binds: gated_backend` (ADR-0171) |
| Freeze start | `2026-09-16` (`performance_freeze` in `work_packages.yaml`) |

One release build of the daemon and the runner was produced on E2 and copied
byte for byte to N1, as the WP-674 rules require; both campaigns record the
same binary digests.

## Receipts

| Profile | Receipt | Path | SHA-256 |
|---|---|---|---|
| n1 | interactive-c32 | `release/evidence/wp-762/n1/interactive-c32.json` | `7adeb3d63e5ab16a9fed7870e643304f1b6d095725f421eb10bd637c5ee7ba5f` |
| n1 | write-only-sweep | `release/evidence/wp-762/n1/write-only-sweep.json` | `6e7cb3368b694c6d69302b452e380d9ccf28d5ff03a7b7a944944210386af63f` |
| e2 | interactive-c32 | `release/evidence/wp-762/e2/interactive-c32.json` | `5db6f775395f38fd380b92308237169b284006559b42591edfb39f13606834fc` |
| e2 | write-only-sweep | `release/evidence/wp-762/e2/write-only-sweep.json` | `86b4a17c5876f2d9396920f049b8428ea2ef8037e70dead4e7fcd30a3b535942` |
| n1+e2 | unary manifest-v1 | `release/evidence/wp-674/manifest-v1.json` | `a819d507025a1b48dfc1d29259eef28c8787a11e75a82328889f9b241504fd0a` |
| n1+e2 | unary baseline-v1 | `release/evidence/wp-674/baseline-v1.json` | `665a2d5c90881ed06c01e02c6b249c7c11f0b559dbad5826b05656d16261b728` |

The unary matrix is the WP-674 manifest and baseline assembled with
`--require-both-profiles`; the workstation column is not part of an ADR-0183
bank because ADR-0142's unary gates are the N1 and E2 absolute service levels.

## Banked values

Interactive mixed workload, 32 clients, three 90-second repetitions after 15
warmup seconds, same-run safe-application PostgreSQL:

| Profile | RiffDB ops/s | Throughput ratio | RiffDB p95 | p95 ratio | Seed ratio |
|---|---:|---:|---:|---:|---:|
| N1 | 5,784 | 0.705x | 31.46 ms | 2.069x | 5.76x |
| E2 | 5,472 | 0.687x | 30.41 ms | 1.871x | 3.00x |

Write-only concurrency sweep, throughput ratio / p95 ratio against
safe-application PostgreSQL at each client point:

| Profile | 1 | 8 | 32 | 128 |
|---|---:|---:|---:|---:|
| N1 | 0.677x / 1.241x | 0.408x / 2.190x | 0.416x / 1.789x | 0.648x / 5.760x |
| E2 | 0.850x / 0.920x | 0.423x / 2.211x | 0.408x / 2.074x | 0.768x / 4.667x |

Unary matrix: `release/evidence/wp-674/baseline-v1.json` holds the frozen
low-water p50 and p95 per scenario and profile; `check-wp674-unary-baseline`
is the verifier. The freeze's activation arithmetic (ADR-0183 section 4)
compares a WP-763 candidate against these values: c32 throughput at least
0.90x, c32 p95 at most 1.25x, write-only p95 within five percent of this bank,
and no unary p50 or p95 above 1.10x its low-water value.

## Method notes

- Both profiles were measured from one release build of the daemon and the runner produced on E2 at revision `48bcb5ce` and copied byte for byte to N1; every receipt records the same `riffdbd`, runner, harness, lock, and source identities, and the unary campaign identities match the load receipts.
- The bank was taken after WP-784 (commit 048a2728) removed the quadratic fresh-locator seal that had collapsed group commit on the 2026-09-15 attempts, and after ADR-0171 Amendment 2 (accepted in session on 2026-09-15): the minimal disclosure cell binds five positive generations for both backends and publishes RiffDB's spreads as disclosure, and kernel threads are excluded from the host-validity process-identity rule while remaining under the CPU and I/O interference thresholds.
- Host preparation on both GCP instances for the run: the OSConfig agent, the workload-certificate refresh timer, and the apt and motd timers were stopped; sshd was firewalled to the operator's address to stop internet login scans from spawning processes inside validity windows; on E2 the virtio memory balloon driver was unloaded after its work function was observed hogging a kernel worker. These are host-state changes, not receipt edits, and every earlier failed campaign is retained on the hosts.
- The PostgreSQL ticket-detail comparator was aligned to the RiffDB `TicketPage` projection (assignee as `user_id` and `display_name`) so both backends execute one canonical semantic payload; before that alignment the unary verifier rejected every `ticket_detail_page` receipt, including the retained 2026-08-30 N1 column, on a 21-byte response-size difference.
- Absolute service level disclosed under `absolute_ceiling_misses` in `baseline-v1.json`: E2 `board_page_450` p50 6.28 ms against the ADR-0142 6.00 ms ceiling for wide bounded named reads (generations 6.13 to 6.37 ms). N1 meets every ceiling. The bank records the values as measured; the ADR-0142 release gate keeps its own refusal.
- Seed ratio disclosed: N1 5.76x against the PERF-008 5.0x ceiling (E2 3.00x). The seed is the harness's bulk load through the public gRPC path and is published as disclosure beside the c32 values; the PERF-008 gate keeps its own refusal.
- Residual write-path gap against the WP-644 baseline: c32 throughput 0.71x (N1) and 0.69x (E2) against 0.88x on both, with c32 p95 2.07x and 1.87x against 1.38x and 1.23x. The E2 probe in `docs/performance/wp-784-fresh-locator-linear-seal.md` attributes the remainder to linear per-command work added since 2026-09-05, chiefly the WP-772 receipt capture; it is a follow-up package, not part of this bank.
- Superseded in one direction since the bank, 2026-09-17: ADR-0236 removed ADR-0197 fresh-locator coverage, which on E2 lowered mean group commit by 15.5 to 18.3 percent and raised write-only throughput by 6.0 to 14.9 percent against the merge base, measured in `docs/performance/wp-787-fresh-locator-coverage-removal.md`. The banked values above are unchanged and remain the frozen evidence for revision `48bcb5ce`, but the c32 ratios and the residual write-path gap they record predate that removal, so they understate the current engine. Any qualification decision that turns on the 0.90x c32 threshold needs a fresh bank rather than these numbers.

## Verification

```bash
./scripts/check-wp762-baseline-bank
./scripts/check-wp674-unary-baseline \
  --manifest release/evidence/wp-674/manifest-v1.json \
  --output /tmp/wp674-baseline-recheck.json
./scripts/check-performance-freeze
```
