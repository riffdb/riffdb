# WP-749 archive-presence evidence, 2026-09-21

## Identity and interpretation

The archive-disabled and archive-enabled arms execute identical binaries. The only arm selector is automatic archive collection. Source revision `11f1ffbc3f1ee1d579e971c3a0e694317ebc1a7d`, tree `150c4c0d1e84c62bca15e5d053c47e1a6df9d921`; Rust 1.98.1, generic x86-64, thin LTO and one codegen unit. Host: 8 vCPU AMD EPYC 9B14; SHA-NI, AES, PCLMULQDQ, SSE4.2.

- Daemon SHA-256: `4d8f7c1991cc4e40d8aefa71d7d686e48e8712ee60c4dbfd2269444cb6e080e5`.
- Runner SHA-256: `6746d28d9173b9b5e527c67893aff8a60e866d28435a829f0f8c25f00105f0d3`.

This is the c32 full-dataset archive comparison, not the PostgreSQL or full replication release qualification. Fifteen seconds of warmup and ninety seconds of measurement run in a fresh database per cell. The full backup is prepared outside timing. Writer counters cover the process workload including warmup; throughput and request percentiles cover measured requests.

## First host attempt: invalid, retained

Five cells were attempted, four passed postflight, and two write-only pairs completed. No interactive cell ran. Cell 5 returned exit 0 but postflight observed two new user processes (`sshd-session`, `sshd-auth`). It recorded no active interfering process and zero whole-cell CPU steal. The SSH journal records a preauthentication close. A later peer comparison differed from the active observer connection; it does not identify the remote actor. Monitoring was initially suspected, but that attribution is not established. The process-identity rule still invalidates the attempt.

All five reports remain in this evidence, including the rejected fifth report. No subset qualifies the package.

| Cell | Archive | Valid cell | Ops/s | P50 ms | P95 ms | P99 ms |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| 01-write_only-1-disabled | disabled | true | 2934 | 9.437 | 11.534 | 96.469 |
| 02-write_only-1-enabled | enabled | true | 2696 | 9.961 | 12.583 | 121.635 |
| 03-write_only-2-enabled | enabled | true | 2668 | 9.961 | 12.583 | 130.023 |
| 04-write_only-2-disabled | disabled | true | 2938 | 9.437 | 11.534 | 96.469 |
| 05-write_only-3-disabled | disabled | false | 2939 | 9.437 | 12.059 | 92.275 |

Both completed first-attempt pairs show lower archive-enabled throughput (8.11% and 9.19%) and higher P95 (9.09% each). These observations are incomplete disclosure, not a five-generation qualification.

## Replacement rule

ADR-0143 section 3 permits at most one whole-host replacement when a predeclared validity check fails independently of performance. The single replacement restarts all twenty cells with the exact same binaries. None of the original four valid cells is imported into the new set. A second invalid attempt ends this campaign. The unchanged host checks include user-process identity stability; neither SSH processes nor unknown outside work are exempted. The replacement observer is one persistent connection present before the first preflight.

## Evidence integrity

`attempt.json` contains each attempt, execution result, preflight/postflight, validated summary when present, original raw-report SHA-256 and exported-report SHA-256. Report extracts retain correctness, outcome reconciliation, request percentiles, writer/archive counters and eligibility flags. `SHA256SUMS` hashes the exported bytes. Original reports and logs have a verified second copy in the private evidence bank; they are not committed because they contain local machine paths. An exported report is never assigned the raw report's checksum.

The runner's generic full-comparison eligibility remains false because this protocol deliberately omits PostgreSQL. Archive-specific correctness and host checks are recorded separately. Terminal archive lag is an inference from the backup frontier plus process command counts, not an independent read of the source frontier; caught-up status is not claimed.

## Replacement host attempt: complete, archive overhead remains

All twenty cells passed their host, correctness and archive-state checks. Both arms used the same daemon and runner digests listed above. Every P50/P95 central-three spread passed the predeclared 1.20 stability ceiling. All whole-cell CPU-steal observations were zero. This establishes a complete stable comparison; it does **not** establish between-arm parity.

The replacement ran from 2026-09-21T13:12:04.745409+00:00 through 2026-09-21T14:11:35.440844+00:00. The host was released at 14:12:32 UTC, within the existing reservation. No further measurement was run.

| Workload | Metric | Archive off median | Archive on median | Change |
| --- | --- | ---: | ---: | ---: |
| write_only | throughput (ops/s) | 2933 | 2684 | -8.49% |
| write_only | request P50 (ms) | 9.437 | 9.961 | +5.56% |
| write_only | request P95 (ms) | 12.059 | 12.583 | +4.35% |
| write_only | request P99 (ms) | 96.469 | 121.635 | +26.09% |
| write_only | mean group commit (ms) | 3.219 | 3.482 | +8.19% |
| interactive | throughput (ops/s) | 15304 | 14599 | -4.61% |
| interactive | request P50 (ms) | 0.688 | 0.754 | +9.52% |
| interactive | request P95 (ms) | 9.961 | 9.961 | +0.00% |
| interactive | request P99 (ms) | 12.059 | 12.583 | +4.35% |
| interactive | mean group commit (ms) | 2.938 | 2.890 | -1.62% |

These are ratios of the five-run arm medians. The committed `summary.json` separately retains every within-pair difference and all five arm values. Every complete pair in both workloads has lower archive-enabled throughput. Each write-only pair also has higher P95 with archiving enabled. There is no accepted archive-overhead allowance implied by the 20% within-arm stability rule. PERF-018's comparator ratios are not reassigned to this off/on comparison.

P99 is disclosure under ADR-0249: +26.09% for write-only and +4.35% for interactive. The earlier correlated investigation named checkpoint-conditioned durability/publication drains. This off/on comparison has no request-correlated observers and does not assign its additional archive tail movement to that mechanism. It opens no new tail hypothesis and introduces no numeric P99 gate.

All ten enabled cells made confirmed archive progress beyond the backup frontier, retained a manifest digest and reported no collector terminal failure. Inferred terminal application-sequence lag was zero in eight cells, 32 in one and 49 in one. The lag method includes warmup command commits; it does not claim independently verified caught-up status. All twenty measured outcome sets had zero errors and zero unavailable outcomes.

### Every replacement cell

| Cell | Archive | Ops/s | P50 ms | P95 ms | P99 ms |
| --- | --- | ---: | ---: | ---: | ---: |
| 01-write_only-1-disabled | disabled | 2921 | 9.961 | 12.059 | 96.469 |
| 02-write_only-1-enabled | enabled | 2684 | 9.961 | 12.583 | 125.829 |
| 03-write_only-2-enabled | enabled | 2659 | 9.961 | 12.583 | 121.635 |
| 04-write_only-2-disabled | disabled | 2931 | 9.961 | 12.059 | 92.275 |
| 05-write_only-3-disabled | disabled | 2933 | 9.437 | 12.059 | 92.275 |
| 06-write_only-3-enabled | enabled | 2714 | 10.486 | 13.107 | 121.635 |
| 07-write_only-4-enabled | enabled | 2682 | 10.486 | 12.583 | 121.635 |
| 08-write_only-4-disabled | disabled | 2938 | 9.437 | 12.059 | 100.663 |
| 09-write_only-5-disabled | disabled | 2951 | 9.437 | 12.059 | 96.469 |
| 10-write_only-5-enabled | enabled | 2705 | 9.961 | 12.583 | 130.023 |
| 11-interactive-1-disabled | disabled | 15304 | 0.688 | 9.437 | 12.059 |
| 12-interactive-1-enabled | enabled | 14599 | 0.754 | 9.961 | 12.583 |
| 13-interactive-2-enabled | enabled | 14633 | 0.754 | 9.961 | 12.583 |
| 14-interactive-2-disabled | disabled | 15308 | 0.688 | 9.961 | 12.059 |
| 15-interactive-3-disabled | disabled | 15350 | 0.688 | 9.437 | 12.059 |
| 16-interactive-3-enabled | enabled | 14559 | 0.754 | 9.961 | 12.583 |
| 17-interactive-4-enabled | enabled | 14642 | 0.754 | 9.961 | 12.583 |
| 18-interactive-4-disabled | disabled | 15096 | 0.655 | 9.961 | 12.583 |
| 19-interactive-5-disabled | disabled | 15066 | 0.688 | 9.961 | 12.583 |
| 20-interactive-5-enabled | enabled | 14486 | 0.754 | 9.961 | 13.107 |

### Disposition

WP-749 remains open. Recovery, source write availability and the complete integration CI have passing proofs, but the measured write-only request P50/P95 and mean group-commit increases do not support the exit gate's claim that archive presence leaves acknowledgement latency unchanged. Closing that gate requires reducing the observed cost or an explicitly accepted clarification of the gate; this evidence does neither. Capture representation, hashing and allocation experiments remain closed. WP-748 is complete independently.

### Reproduce and verify

Run `sha256sum -c SHA256SUMS` from this directory to verify the committed bytes. Run `python3 summarize.py` to verify report links and outcome reconciliation, then recompute both attempts' medians, spreads and paired differences. Its output must match `summary.json`. The incomplete first attempt is never combined with the replacement.

Package: WP-749. Tier: surface, from the `release/**` path rule. Existing ADR-0143, ADR-0171 and ADR-0249 govern this evidence; their decisions are unchanged. Behavior: records the fixed comparison and its unresolved latency cost; no runtime change. Checks: verified 54 first-attempt and 159 replacement raw evidence files against their manifests, independently recomputed all reported arithmetic, and scanned exports for machine addresses and personal paths. Compatibility: no protocol, durable format or guarantee changed. Hazards and follow-ups: archive acknowledgement overhead remains open; source-level attribution and further measurements require separate work.
