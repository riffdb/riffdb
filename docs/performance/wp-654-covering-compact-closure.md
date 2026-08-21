# WP-654 covering results and compact carriage

Status: closed honestly under ADR-0133's independent-activation rule. The
combined candidate is retained, but WP-654 does not claim its conjunctive
`BoardPage450 <= 1.10x PostgreSQL` gate passed.

## Frozen scope

WP-654 adds compiler-owned covering-index metadata and derivation, one sealed
covered-result batch, negotiated compact named-result carriage, and generated
decoders in Rust, Go, TypeScript, and Python. Applications still invoke only a
finite generated named query. They cannot select the index, cover, positional
layout, wire arm, policy, consistency, validation, or fallback behavior.

The production candidate is commit `235f5f6984dd072d40aed30dbc746e5f9a0efd29`.
Existing contracts and clients retain their legacy identities and result arm.
Only a newly compiled eligible plan can produce non-empty cover state, and a
new client negotiates compact carriage while remaining able to validate the
legacy arm.

## Combined gate result

The WP-653 control was the ordinary hydrated named-result path. The retained
candidate produced these `BoardPage450` p50 values:

| Profile | WP-653 | WP-654 | Improvement | Safe PostgreSQL | RiffDB / PG |
|---|---:|---:|---:|---:|---:|
| Workstation | 3.288 ms | 1.439 ms | 56.2% | diagnostic only | — |
| N1 | 8.042 ms | 4.730 ms | 41.2% | 2.527 ms | 1.87x |
| E2 | 7.752 ms | 4.506 ms | 41.9% | 2.533 ms | 1.78x |

The candidate clears the required 40% improvement on both cloud profiles but
does not reach the conjunctive 1.10x comparator threshold. That miss is
reported as a miss; it is not converted into a WP-654 or alpha performance
pass.

The retained current-head page controls were:

| Profile | 50 rows | 200 rows | 450 rows |
|---|---:|---:|---:|
| Workstation | 0.345 ms | 0.757 ms | 1.439 ms |
| N1 | 1.574 ms | 2.768 ms | 4.730 ms |
| E2 | 1.830 ms | 2.884 ms | 4.506 ms |

## Independent activation split

ADR-0133 permits compact carriage to remain after a combined-gate miss only
with an independently measured customer-path improvement of at least 20% and
no semantic regression. A diagnostic build advertised only the frozen legacy
result arm while leaving the compiler-selected cover and positional execution
unchanged. It was never committed and creates no caller-selectable product
option.

| Profile | Cover + legacy | Cover + compact | Compact improvement |
|---|---:|---:|---:|
| Workstation | 2.002 ms | 1.439 ms | 28.1% |
| N1 | 6.728 ms | 4.730 ms | 29.7% |
| E2 | 6.580 ms | 4.506 ms | 31.5% |

Compact carriage therefore clears its independent activation threshold on all
three profiles. Legacy and compact arms remain semantically identical through
the four generated-language suites and malformed/mixed-arm protocol tests.

The same split also measures covering execution without compact carriage.
Relative to the WP-653 hydrated path, covering alone improves
`BoardPage450` 39.1% on the workstation, 16.3% on N1, and 15.1% on E2. Cover
production is retained because it improves the declared read on every profile,
is the compiler-sealed prerequisite for the independently qualifying compact
path, and its bounded write cost does not worsen the full seed:

| Profile | Pre-WP-654 seed | Current seed | Change |
|---|---:|---:|---:|
| N1 | 7.009 s | 6.987 s | 0.3% faster |
| E2 | 7.587 s | 6.605 s | 12.9% faster |

A final sequential N1 control removed cross-run host noise from the write
gate. The pre-candidate commit and retained candidate ran back-to-back on the
same warmed host with PostgreSQL skipped and identical 100-sample/20-warmup
settings:

| Scenario | Pre-candidate | Retained | Change |
|---|---:|---:|---:|
| Full seed | 7.828 s | 7.621 s | 2.6% faster |
| `create_comment` | 4.341 ms | 4.093 ms | 5.7% faster |
| `close_ticket_with_comment` | 4.143 ms | 4.076 ms | 1.6% faster |
| `swap_member_roles` | 3.729 ms | 3.604 ms | 3.4% faster |
| `open_ticket_with_labels` | 4.421 ms | 4.308 ms | 2.6% faster |
| `BoardPage450` | 8.762 ms | 4.963 ms | 43.4% faster |

This paired control is diagnostic rather than PERF-018 release evidence, but
it directly falsifies a covering-write or seed regression on the measured N1
profile.

All cover bytes are derived atomically from transaction-current post-images.
Startup/recovery structural validation, update/delete derivation, changelog,
backup/restore, malformed cover, mixed-version, and byte-frozen compatibility
tests remain fail closed. No query falls back to entity hydration when an
eligible cover is missing or invalid.

## Rejected follow-on

A later internal experiment reused verified cover bytes and moved uniquely used
key/cover values instead of cloning them. It improved the workstation 450-row
page 10.4% and N1 8.5%, but regressed E2 5.4% against the retained paired
receipt. That crosses the package's 5% control boundary. The experiment was
removed completely; no part of it is activated or committed.

## Receipts

| Receipt | SHA-256 |
|---|---|
| WP-653 workstation control | `8a7fa1b2ee16661564859d1114d03e576f2dc497fd67280db65cbd32d362017e` |
| WP-649 N1 pre-candidate | `50a26ccd5e5eaec36d2052b32b83c9d5382ffaaf42b46d800d4670afa8ecc203` |
| WP-649 E2 pre-candidate | `bd18a03889249e016fcc95ad5fbccef632db0072819a628c8280922340f1cd5b` |
| Workstation compact ledger | `6b917ff1b09635c2d47edd90ea921d8dd96c427728c3200dab61b023e5204db6` |
| N1 retained paired result | `8a7582fa17e41301b0b6bd39e8cf0f06634c46ca4e52ccc515101f615a703ef2` |
| E2 retained paired result | `5de39c337c6143b99c23e1f74fe0e036889ae89da3027de0388c8a34f14c4f80` |
| Workstation cover + legacy split | `a723686e6708ddbbc8ed92bfe9651e1368c8e85cfd3e1e3caa46d40b2ef6f1e9` |
| N1 cover + legacy split | `1e08087437e94a4fef237ce68146c8fdc8c3c3b9a8ef66c618909e35f90cae01` |
| E2 cover + legacy split | `35afaa46e5fb2b4cd2d911c7715e0e0991ef1142a93a74fde3cb006fa7f4332d` |
| N1 sequential pre-candidate control | `88b446486deb0a08230800890f0a7d2beacc82c9ce83b4706e03c7e095877216` |
| N1 sequential retained control | `062eb34d8ad8143b55b1c3d0d50a263bb5aa61268ea2dbcc5fbc870c0e78fbec` |

The raw receipts remain outside the repository under `/home/kevin/tmp` and
contain no application values or credentials. WP-623 owns the final 90-second
same-run qualification of the unchanged artifact; these shorter WP-654
receipts cannot be promoted into PERF-018 release evidence.
