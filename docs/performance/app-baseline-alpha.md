# App-baseline alpha evidence

The alpha benchmark is a correctness-bearing application suite, not a database
microbenchmark and not a single leaderboard number. It compares the same
TicketDesk operations through PostgreSQL and RiffDB's public symbolic
application surface, then separately tests sustained demand and recovery.

## Canonical commands

```bash
# Warm parity floors and safety-equivalent comparison
./benchmarks/run-app-baseline --full --postgres-comparator minimal --reps 3 --require-stable
./benchmarks/run-app-baseline --full --postgres-comparator safe-app --reps 3 --require-stable

# Closed-loop demand with semantic canaries
./benchmarks/run-app-baseline --full --load interactive --load-clients 32 \
  --load-duration-secs 90 --load-warmup-secs 15 --load-journeys \
  --reps 3 --require-stable

# Diagnostic capacity isolation (same public application surface)
./benchmarks/run-app-baseline --full --load read_only --load-concurrency-sweep \
  --load-duration-secs 90 --load-warmup-secs 15 --reps 3 --require-stable
./benchmarks/run-app-baseline --full --load write_only --load-concurrency-sweep \
  --load-duration-secs 90 --load-warmup-secs 15 --reps 3 --require-stable

# Open-loop offered load
./benchmarks/run-app-baseline --full --load agent --load-clients 32 \
  --load-open-loop-rate 2000 --load-open-loop-queue-depth 4096 \
  --load-duration-secs 90 --load-warmup-secs 15 --reps 3 --require-stable

# Partition fairness and a hot tenant
./benchmarks/run-app-baseline --full --organizations 64 --load interactive \
  --load-clients 32 --load-tenants 64 --load-hot-tenant-percent 70 \
  --load-duration-secs 90 --load-warmup-secs 15 --reps 3 --require-stable

examples/app-baseline/resilience/run
./scripts/app-baseline-language-conformance
./benchmarks/run-app-baseline --alpha-matrix
```

All temporary build/test roots honor `RIFFDB_TMP_ROOT` and default to
`$HOME/tmp`; database evidence lives under `target/perf-db`, never `/tmp`.

## Versioned schemas

| Schema | Purpose |
|---|---|
| `riffdb.app-baseline/v1` | parity report |
| `riffdb.app-baseline-load/v1` | one backend/client load point |
| `riffdb.app-baseline-load-suite/v1` | repetitions, curve, environment, correctness |
| `riffdb.app-baseline-performance-sentinel/v1` | short, RiffDB-only, non-evidentiary regression receipt |
| `riffdb.app-baseline-resilience/v1` | process/failpoint recovery cells |
| `riffdb.app-baseline-language-conformance-result/v1` | generated-client semantic/boundary result |
| `riffdb.app-baseline-alpha-matrix/v1` | required cells and evidence hashes |
| `riffdb.app-baseline-qualified-candidate/v1` | clean source, lock, harness, runner, and daemon identity attached to an evidentiary report |
| `riffdb.app-baseline-host-validity/v1` | immutable historical bounded endpoint host inventory |
| `riffdb.app-baseline-host-validity/v2` | bounded endpoint inventory plus exact whole-cell CPU-steal evidence |
| `riffdb.wp-674-unary-profile/v2` | one fixed host profile and every retained attempt |
| `riffdb.wp-674-unary-manifest/v2` | exact three-profile unary evidence inventory |
| `riffdb.wp-674-unary-baseline/v2` | frozen absolute and low-water unary baseline |
| `riffdb.wp-674-unary-qualification/v2` | derived unary regression qualification |
| `riffdb.wp-623-performance-qualification-manifest/v2` | content-addressed workstation/cloud qualification inventory |
| `riffdb.wp-623-performance-qualification/v2` | derived gate ratios and exact qualified source identity |

Adding optional fields is compatible within v1. Removing/renaming a field,
changing units, changing a percentile population, changing an operation mix,
or changing eligibility meaning requires a new schema version. Every duration
field names its unit (`_ns`, `_ms`, `_secs`).

## Eligibility rules

The full matrix is eligible only when:

- every required parity/load/resilience/language cell exists and its hash is
  recorded;
- both engines use durable settings and comparable physical media;
- load windows are at least 60 seconds with at least two repetitions;
- repetition summaries are stable;
- no public unavailable, idempotency mismatch, decoding error, semantic
  journey divergence, or recovery failure occurred;
- every load point has bounded resource attribution;
- the generated Rust, TypeScript, and Python surfaces pass boundary checks and
  agree on the canonical semantic observation.

`--require-stable` additionally requires an exact clean source tree and bounded
host-validity inventories before and after every measured run, including
sample-based parity runs. A process outside the harness crossing
the frozen CPU or I/O threshold yields the typed reason
`host_interference`; a preflight refusal skips measurement, while a postflight
finding preserves the raw result but makes it non-evidentiary. Process arguments
are never retained. Successful reports bind the source revision, `Cargo.lock`,
benchmark harness, built runner, and `riffdbd` digests; evidence from a nearby
binary or a dirty checkout cannot be substituted later.

## WP-674 unary baseline and WP-623 three-profile qualification

Unary qualification is banked independently for a workstation with local NVMe,
an N1 cloud VM with persistent disk, and an E2 cloud VM with persistent disk.
The initial bank freezes the 32-logical-CPU Ryzen 9 7950X workstation, the
8-vCPU Intel N1 host, and the 8-vCPU AMD EPYC 7B12 E2 host, including bounded
OS/system identity and persistent-device metadata. Candidate evidence must
match those stable hardware identities.
For each of the fourteen frozen unary operations, the harness runs five fixed
counterbalanced process generations. Each generation restarts RiffDB after
common setup, performs 20 same-operation warmups, then records exactly 1,000
operations.
All five p50 and p95 observations are retained. After sorting them as
`x1 <= x2 <= x3 <= x4 <= x5`, the qualified statistic is `x3` and stability
requires `x4 / x2 <= 1.20` independently for each backend and percentile. The
two extremes remain disclosed. PostgreSQL ratios are derived from the two
qualified backend medians and are not separately stability-gated.

Each measured cell also binds V2 preflight and postflight host observations.
The postflight proves the complete-cell aggregate Linux CPU-steal delta against
the exact preflight; more than 1.00 percent steal, counter or boot/host drift,
or either bounded process/IO inventory failure makes the attempt
non-evidentiary. This ceiling is fixed by the release protocol and cannot be
selected per caller or profile.

There is one fixed measurement set per host attempt. A completed unstable cell
fails and is never retried. The entire host attempt may be replaced once only
when a predeclared host-validity failure independent of performance is retained
in the receipt; a second invalid host attempt fails. Runtime, correctness,
identity, topology, and evidence-shape failures stop the profile. The verifier
rejects per-scenario retries, performance-selected replacement, a third host
attempt, or an accepted attempt whose bytes differ from the canonical reports.

The frozen classes are:

| Class | Operations | p50 | p95 |
|---|---|---:|---:|
| ordinary named read | seven point/list/detail operations | 3 ms | 6 ms |
| wide bounded named read | board 50/200/450 | 6 ms | 12 ms |
| compiled command | four representative commands | 10 ms | 15 ms |

The absolute ceilings gate N1 and E2. All three profiles additionally gate each
operation/statistic against the exact WP-674 RiffDB low-water bank at no more
than `1.10x`. Safe-application PostgreSQL runs in every unary cell and minimal
PostgreSQL is retained as a full disclosure report. Their p50/p95 ratios remain
published evidence, not unary release gates.

Every unary row also carries exact request and response bytes under the frozen
`riffdb.app-baseline-canonical-semantic-frame/v1` encoding. This counts the
backend-neutral application payload (fixed-width UUID/integer/enum values,
length-delimited strings and collections, and explicit optional/outcome tags),
not HTTP/2 or PostgreSQL wire framing. The verifier requires equal sizes across
RiffDB and PostgreSQL so latency cannot be compared across different semantic
payloads.

Bank one clean host column with:

```bash
./benchmarks/run-wp674-unary-profile \
  --profile workstation \
  --output-dir release/evidence/wp-674/workstation
```

Use `--profile n1` and `--profile e2` on those hosts. Assemble and verify the
three columns only after copying them beneath one evidence root:

WP-674 requires one exact runner and daemon artifact across all three hosts.
Build them once, copy those bytes to the other hosts, and set
`RIFFDB_APP_BASELINE_RUNNER_BIN` and `RIFFDB_APP_BASELINE_RIFFDBD_BIN` there;
the final verifier rejects even a source-equivalent binary with another digest.

```bash
./scripts/assemble-wp674-unary-manifest \
  --workstation release/evidence/wp-674/workstation/profile-fragment.json \
  --n1 release/evidence/wp-674/n1/profile-fragment.json \
  --e2 release/evidence/wp-674/e2/profile-fragment.json \
  --output release/evidence/wp-674/manifest-v1.json

./scripts/check-wp674-unary-baseline \
  --manifest release/evidence/wp-674/manifest-v1.json \
  --output release/evidence/wp-674/baseline-v1.json
```

The final optimized alpha candidate must then pass on those same three
profiles. Each profile retains four full-matrix reports from one exact clean
source revision:

- full safe-application PostgreSQL parity, three repetitions;
- full minimal PostgreSQL parity, reported as the conventional floor;
- counterbalanced interactive load at 1, 8, 32, and 128 clients, three
  90-second post-warmup repetitions; and
- the matching write-only sweep.

The safe-application report is the mixed/seed gate comparator. The full seed
must remain at most `5.0x`, and the interactive 32-client cell must provide at
least `0.90x` PostgreSQL throughput with at most `1.25x` PostgreSQL p95.
WP-623 also consumes a fresh WP-674 unary qualification receipt against the
frozen bank. Minimal PostgreSQL is verified for identical durability, source,
host, storage, stability, and correctness, but does not replace the
safe-application mixed/seed gate.

The retained manifest is checked with:

```bash
./scripts/check-wp623-performance-qualification \
  --manifest release/evidence/wp-623/manifest-v2.json \
  --output release/evidence/wp-623/qualification-v2.json
```

The verifier rejects an omitted profile or report, hash or source identity
mismatch, dirty source, interference, instability, shortened window,
client/workload/transport/durability drift, incomplete matrix, correctness
failure, missing p50/p95 disclosure, unary reclassification, baseline
regression, or failed absolute/mixed/seed gate. A digest alone never qualifies
a run.

## WP-552 evidence status

WP-552 completed its idle-host evidence on 2026-08-09. Both the interactive and
write-only corpora contain three counterbalanced, isolated 90-second
repetitions at 1, 8, 32, and 128 clients for RiffDB public gRPC and the frozen
safe-app PostgreSQL comparator. Both reports are stable, correctness-clean,
same-device comparable, host-idle, and eligible.

Median throughput in operations/second:

| Profile / clients | PG safe-app | RiffDB | RiffDB / PG |
|---|---:|---:|---:|
| interactive / 1 | 2,581 | 1,909 | 0.74× |
| interactive / 8 | 13,538 | 13,321 | 0.98× |
| interactive / 32 | 39,286 | 32,902 | 0.84× |
| interactive / 128 | 32,024 | 38,929 | 1.22× |
| write-only / 1 | 563 | 475 | 0.84× |
| write-only / 8 | 1,651 | 2,621 | 1.59× |
| write-only / 32 | 5,964 | 5,794 | 0.97× |
| write-only / 128 | 4,131 | 6,369 | 1.54× |

The write-only c128 median p99 was 48.2 ms for RiffDB and 906.0 ms for the
safe-app comparator. That result describes this exact same-device workload;
it is not a general PostgreSQL latency claim.

The raw merged reports and their earlier typed `host_interference` refusal
receipts are retained under `release/evidence/wp-552/` with SHA-256 hashes.
The alpha release gate consumes them through
`./scripts/check-alpha-performance-evidence --verify`; the verifier checks the
content-addressed two-profile inventory and the full repetition/concurrency,
duration, safe-app durability, idle-host, stability, and correctness contract.
It does not treat a file hash alone as evidence eligibility.
The refusal receipts remain useful proof that an interfered host cannot produce
publishable evidence.

Smoke matrix success means only that the orchestration works. Its manifest sets
`eligible: false` by construction.

## Interpretation and claim boundaries

`postgres_minimal` is the optimized conventional SQL floor.
`postgres_safe_app` adds transactional authorization, idempotency, audit,
domain-event, and outbox obligations. PostgreSQL can implement these semantics;
the second profile measures their cost rather than claiming otherwise.

RiffDB load timings use public named RiffQL and symbolic commands. They never
use storage/kernel APIs, numeric schema IDs, field masks, or encoded keys.
Language runtime/package setup is reported independently and excluded from
database ratios.

Closed-loop throughput measures completions under self-throttled clients.
Open-loop throughput measures offered/admitted/completed demand and queueing.
Neither substitutes for the other. The default concurrency sweep starts every
point from a fresh identical seed; `--load-accumulate-history` deliberately
changes that question and is labeled as history growth.

The current application load uses deployed static page bounds (board
50/200/450, comments 50). Generated-client cursor resume is conformance
evidence, not silently counted as an arbitrary deep-page load result.

`read_only` contains only named RiffQL operations. `write_only` contains only
symbolic application commands, including child append, root mutation, and root
creation. `append_only` isolates compiler-proved `CreateComment` child appends
to measure ADR-0094 shared-conflict grouping without root-command compatibility
cuts. They are diagnostic profiles, not alternative product semantics: use
them to distinguish read/application-port saturation, mixed-command grouping,
and the sole durable writer ceiling before interpreting a mixed curve.

Each RiffDB point reports successful mutation and command-attempt rates,
process and durable byte growth per command committed by the complete measured
daemon generation (including warmup), a closed authoritative-table inventory,
and a closed writer evidence block. The table inventory compares the
post-seed/pre-warmup database with clean shutdown and reports row, stored-byte,
tree, page, metadata, and fragmentation values. After the measured daemon
stops, the harness must reopen the complete checkpoint-plus-journal durable
unit once before acquiring the frozen inventory view. Reopen or inventory
failure fails the cell; neither is retried and no partial inventory is
reported. Those page values attribute retained footprint; the process counter
remains the physical-I/O total. The writer block includes intake selection/defer counts,
compatibility splits, compiler-proved shared-conflict group count, queue delay,
writer busy/idle time, physical commit and durable-flush histograms, and logical
commands per physical commit. Labels are fixed and contain no contract, tenant,
key, principal, or submitted value.

Use the explicitly named
`process_write_bytes_per_process_scope_committed_command` and
`durable_bytes_growth_per_process_scope_committed_command` fields for resource
comparison. The legacy v1 `*_per_successful_mutation` fields are retained for
schema compatibility but combine different measurement scopes.

The resilience report combines a real killed `riffdbd` under application load
with focused deterministic tests for exact replay, durable-consumer crashes,
authorization revocation, successor-role reconciliation, and retained-history
growth. Focused test wall time is never presented as database throughput.

The matrix locates rather than assumes an offered-load knee by running the
agent profile at 500, 2,000, and 8,000 operations/second. It also includes a
separate accumulated-history 1/8/32/128-client curve; that curve must never be
used as concurrency-only scaling evidence.
