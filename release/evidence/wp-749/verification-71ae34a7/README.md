# WP-749 verification and refused measurement preflight

Package: WP-749 (open). Evidence collected 2026-09-16.

## Correctness verification

`./scripts/ci-all` completed with exit 0 at exact clean revision
`71ae34a7a4c72965c378fb4c2bfabfe910d92b94`. The run included workspace and
xtask formatting/clippy/tests, real archive CLI scenarios, rustdoc, dependency
checks, test-partition coverage, all language adapters, requirement/ADR checks,
all 28 generators, Helm checks, public authoring and source-install smoke.
`ci-all.log.gz` preserves the complete log. Its uncompressed SHA-256 is
`132b840321c83a0283bfb77bdef2000141dc75dfb036b9236a53483557b44ebc`; the source Cargo.lock SHA-256 is
`42067e22a885ca75efc65fff7ba7db5208b49e5ce62d8d636d27082b56995e87`.

The run used Rust 1.97.0, locked TypeScript dependencies, maturin 1.14.1,
PyYAML 6.0.3, four test threads and isolated temporary and Cargo target roots.
The process completed before the host observation below. This is correctness
verification, not a timed performance result.

The later test/docs-only commit `b6073b2cd80c0752f728c4c4b491a8304b40ea68`
adds the required storage-matrix sink-failure arm. Its separate focused run
passes two tests, including four child-process cases (Standard/Hardened crossed
with failure before persistence/loss of confirmation after persistence).
Two source reopens preserve acknowledged command graphs and entities; validated
archive replay recovers only the durable prefix. The sink-unavailable outcomes
are injected at the port around the production filesystem repository.

`sink-matrix.log.gz` preserves that run. `sink-matrix-acceptance.log.gz`
preserves all four passing scoped checks: allowed paths, formatting, clippy and
handbook. The focused tests ran separately from `acceptance --no-tests`.
The full CI run above does not include this later test/docs commit, and these
checks must not be presented as one full CI run at that revision.

## Performance preflight refused

After all task-owned build/test sessions completed, the existing
`scripts/app-baseline-host-validity` ran with its unchanged three-second interval
and thresholds outside the tool sandbox. `host-visibility.json` records host
PID 1 (`systemd`), the process namespace and 1,219 visible processes. No process
arguments or environment values were collected.

The sampler returned exit 3, `valid: false`, `reason: host_interference`.
Four processes exceeded the existing CPU-interference limit, and process churn
was observed. `host-preflight.json` and `host-exit-status.txt` preserve the raw
result. No workload cell started. No other process was stopped, no threshold
changed, and no performance pass or no-regression claim follows from this bundle.

The prepared collector revision is `700265a87b53e58e91244cd110cd16481dda5803`.
Its archive worker, complete-receipt grouping and benchmark wrapper are unchanged
through the CI revision. Prepared binaries are not execution evidence.
The earlier local timing reports remain unqualified; this later observation
cannot reconstruct their interference. WP-749 needs qualified write-path
measurements. WP-748 administration/promotion and WP-750's N1/E2 campaign remain
open. The provider/performance freeze remains in force.

## Reproduce the recorded checks

At the exact CI revision, install the locked build tools and dependencies and
run `./scripts/ci-all`. At the later test revision, run:

```sh
cargo test -p riffdb-storage-redb --test storage_recovery_matrix archive_sink_failure_crash
./scripts/acceptance --wp WP-749 --range b6073b2c^..b6073b2c --no-tests
```

Use independent temporary roots for process tests. Do not run performance
measurements alongside builds/tests or from a process namespace that hides
host activity. From this bundle, `sha256sum --check SHA256SUMS` verifies retained
bytes; `gzip -dc ci-all.log.gz` reads the complete CI log.
